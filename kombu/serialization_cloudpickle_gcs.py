"""
kombu.serialization_cloudpickle_gcs
====================================

CloudPickle serializer with Google Cloud Storage offloading for large payloads.

This serializer uses cloudpickle for enhanced Python object serialization.
Small payloads (< threshold) are sent inline, while large payloads are
automatically uploaded to GCS and a reference is sent instead.

Configuration:
    Use CloudPickleGCSConfig class (programmatic) or environment variables.

    Programmatic:
        from kombu.serialization_cloudpickle_gcs import CloudPickleGCSConfig

        CloudPickleGCSConfig.configure(
            threshold=10 * 1024 * 1024,  # 10MB
            bucket='my-bucket',
            prefix='kombu/',
            ttl_seconds=86400  # 24 hours
        )
"""
from __future__ import absolute_import

import json
import os
import uuid
from datetime import datetime, timedelta

from .exceptions import DecodeError, EncodeError, SerializerNotInstalled
from .five import BytesIO, text_t, bytes_t

__all__ = ['register_cloudpickle_gcs', 'CloudPickleGCSConfig']

# Default Configuration values
CLOUDPICKLE_GCS_THRESHOLD_DEFAULT = 10 * 1024 * 1024 - 1024 # (10MB - 1KB buffer)
CLOUDPICKLE_GCS_BUCKET_DEFAULT = 'kombu-large-payloads'
CLOUDPICKLE_GCS_PREFIX_DEFAULT = 'kombu'
CLOUDPICKLE_GCS_TTL_DEFAULT =  86400  # 24 hours

# Marker for GCS reference payloads
GCS_REF_TYPE = b'__cld_pkl_gcs_ref__'


class CloudPickleGCSConfig(object):
    """Configuration manager for CloudPickle GCS serializer.

    Provides programmatic configuration as an alternative to environment variables.
    Configuration priority: programmatic > environment variables > defaults

    Example:
        # Configure programmatically
        from kombu.serialization_cloudpickle_gcs import CloudPickleGCSConfig

        CloudPickleGCSConfig.configure(
            threshold=20 * 1024 * 1024,  # 20MB
            bucket='my-kombu-bucket',
            prefix='messages/',
            ttl_seconds=172800  # 48 hours
        )

        # Get current values
        threshold = CloudPickleGCSConfig.get_threshold()
        bucket = CloudPickleGCSConfig.get_bucket()

        # Reset to defaults
        CloudPickleGCSConfig.reset()
    """

    # Class-level configuration storage
    _config = {
        'threshold': None,
        'bucket': None,
        'prefix': None,
        'ttl_seconds': None,
    }

    @classmethod
    def configure(cls, threshold=None, bucket=None, prefix=None, ttl_seconds=None):
        """Configure the CloudPickle GCS serializer programmatically.

        Args:
            threshold (int): Size threshold in bytes. Payloads larger than this
                           will be offloaded to GCS
            bucket (str): GCS bucket name
            prefix (str): Object key prefix
            ttl_seconds (int): TTL in seconds for auto-cleanup

        Example:
            CloudPickleGCSConfig.configure(
                threshold=50 * 1024 * 1024,  # 50MB
                bucket='my-bucket',
                prefix='app/messages/',
                ttl_seconds=604800  # 1 week
            )
        """
        if threshold is not None:
            cls._config['threshold'] = int(threshold)
        if bucket is not None:
            cls._config['bucket'] = str(bucket)
        if prefix is not None:
            cls._config['prefix'] = str(prefix)
        if ttl_seconds is not None:
            cls._config['ttl_seconds'] = int(ttl_seconds)

    @classmethod
    def reset(cls):
        """Reset all configuration to use defaults."""
        cls._config = {
            'threshold': None,
            'bucket': None,
            'prefix': None,
            'ttl_seconds': None,
        }

    @classmethod
    def get_threshold(cls):
        """Get configured threshold value.

        Returns:
            int: Threshold in bytes (default: 10MB)
        """
        if cls._config['threshold'] is not None:
            return cls._config['threshold']
        return CLOUDPICKLE_GCS_THRESHOLD_DEFAULT

    @classmethod
    def get_bucket(cls):
        """Get configured bucket name.

        Returns:
            str: Bucket name (default: 'kombu-large-payloads')
        """
        if cls._config['bucket'] is not None:
            return cls._config['bucket']
        return CLOUDPICKLE_GCS_BUCKET_DEFAULT

    @classmethod
    def get_prefix(cls):
        """Get configured object prefix.

        Returns:
            str: Object key prefix (default: 'kombu')
        """
        if cls._config['prefix'] is not None:
            return cls._config['prefix']
        return CLOUDPICKLE_GCS_PREFIX_DEFAULT

    @classmethod
    def get_ttl_seconds(cls):
        """Get configured TTL in seconds.

        Returns:
            int: TTL in seconds (default: 86400 = 24 hours)
        """
        if cls._config['ttl_seconds'] is not None:
            return cls._config['ttl_seconds']
        return CLOUDPICKLE_GCS_TTL_DEFAULT


class GCSStorageBackend(object):
    """Singleton GCS storage backend for managing large payload offloading."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GCSStorageBackend, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.client = None
        self.bucket = None
        self._initialized = True

    def initialize(self):
        """Lazy initialization of GCS client and bucket.

        Raises:
            SerializerNotInstalled: If google-cloud-storage is not available
            EncodeError: If GCS authentication or bucket access fails
        """
        if self.client is not None:
            return

        try:
            from google.cloud import storage
        except ImportError:
            raise SerializerNotInstalled(
                'cloudpickle-gcs serializer requires google-cloud-storage. '
                'Install it with: pip install google-cloud-storage'
            )

        try:
            self.client = storage.Client()
            bucket_name = CloudPickleGCSConfig.get_bucket()
            self.bucket = self.client.bucket(bucket_name)

            # Verify bucket exists and is accessible
            if not self.bucket.exists():
                raise EncodeError(
                    f'GCS bucket {bucket_name} does not exist or is not accessible. '
                    'Please create it or check your credentials.'
                )
        except (EncodeError, SerializerNotInstalled):
            raise
        except Exception as exc:
            raise EncodeError(
                f'Failed to initialize GCS client: {exc}'
            )

    def upload(self, key, data, metadata=None):
        """Upload data to GCS.

        Args:
            key: Object key/path
            data: Bytes to upload
            metadata: Optional dict of metadata

        Returns:
            str: GCS URI (gs://bucket/path)

        Raises:
            EncodeError: If upload fails
        """
        self.initialize()

        try:
            blob = self.bucket.blob(key)

            # Set metadata
            if metadata:
                blob.metadata = metadata

            # Set custom time for lifecycle management
            ttl_seconds = CloudPickleGCSConfig.get_ttl_seconds()
            expiration = datetime.utcnow() + timedelta(seconds=ttl_seconds)
            blob.custom_time = expiration

            # Upload with retry
            blob.upload_from_string(
                data,
                content_type='application/octet-stream',
                retry=self._get_retry_strategy()
            )

            bucket_name = CloudPickleGCSConfig.get_bucket()
            return f'gs://{bucket_name}/{key}'

        except Exception as exc:
            raise EncodeError(
                'Failed to upload to GCS: {0}'.format(exc)
            )

    def download(self, gcs_uri):
        """Download data from GCS.

        Args:
            gcs_uri: GCS URI (gs://bucket/path)

        Returns:
            bytes: Downloaded data

        Raises:
            DecodeError: If download fails
        """
        self.initialize()

        try:
            # Parse GCS URI
            if not gcs_uri.startswith('gs://'):
                raise DecodeError(f'Invalid GCS URI: {gcs_uri}')

            path = gcs_uri[5:]  # Remove 'gs://'
            bucket_name, _, key = path.partition('/')

            expected_bucket = CloudPickleGCSConfig.get_bucket()
            if bucket_name != expected_bucket:
                raise DecodeError(
                    'GCS URI bucket mismatch: expected {0}, got {1}'.format(
                        expected_bucket, bucket_name
                    )
                )

            blob = self.bucket.blob(key)

            # Download with retry
            data = blob.download_as_bytes(retry=self._get_retry_strategy())

            return data

        except Exception as exc:
            raise DecodeError(
                'Failed to download from GCS: {0}'.format(exc)
            )

    def _get_retry_strategy(self):
        """Get retry strategy for GCS operations."""
        try:
            from google.api_core import retry
            return retry.Retry(
                initial=1.0,
                maximum=10.0,
                multiplier=2.0,
                deadline=60.0
            )
        except ImportError:
            return None


# Global GCS backend instance
_gcs_backend = GCSStorageBackend()


def cloudpickle_gcs_dumps(obj, threshold=CLOUDPICKLE_GCS_THRESHOLD_DEFAULT):
    """Serialize object with cloudpickle, optionally offloading to GCS.

    Args:
        obj: Python object to serialize
        threshold: Size threshold for GCS offloading (bytes)

    Returns:
        bytes: Serialized data (either direct or GCS reference)

    Raises:
        EncodeError: If serialization or upload fails
        SerializerNotInstalled: If cloudpickle is not available
    """
    try:
        import cloudpickle, zstandard
    except ImportError:
        raise SerializerNotInstalled(
            'cloudpickle_gcs serializer requires cloudpickle and zstandard. '
            'Install it with: pip install cloudpickle zstandard'
        )

    try:
        # Serialize with cloudpickle
        serialized_data = zstandard.compress(cloudpickle.dumps(obj))
        data_size = len(serialized_data)

        # Check if we need to offload to GCS
        if data_size < threshold:
            # Small payload - return directly
            return serialized_data

        # Large payload - upload to GCS
        unique_id = uuid.uuid4().hex
        prefix = CloudPickleGCSConfig.get_prefix()
        key = f'{prefix}/{unique_id}.cloudpickle'

        # Upload to GCS
        gcs_uri = _gcs_backend.upload(
            key,
            serialized_data,
            metadata={
                'size': str(data_size),
                'serializer': 'cloudpickle_gcs',
                'created': datetime.utcnow().isoformat()
            }
        )

        # Create reference payload
        reference = {
            'type': 'gcs_ref',
            'uri': gcs_uri,
            'size': data_size,
            'key': key
        }

        # Encode reference as JSON with marker prefix
        reference_json = json.dumps(reference).encode('utf-8')
        return GCS_REF_TYPE + reference_json

    except (EncodeError, SerializerNotInstalled):
        raise
    except Exception as exc:
        raise EncodeError(
            f'Failed to serialize with cloudpickle: {excs}'
        )


def cloudpickle_gcs_loads(data):
    """Deserialize data, downloading from GCS if needed.

    Args:
        data: Serialized data (either direct or GCS reference)

    Returns:
        object: Deserialized Python object

    Raises:
        DecodeError: If deserialization or download fails
        SerializerNotInstalled: If cloudpickle is not available
    """
    try:
        import cloudpickle, zstandard
    except ImportError:
        raise SerializerNotInstalled(
            'cloudpickle-gcs serializer requires cloudpickle and zstandard '
            'Install it with: pip install cloudpickle zstandard'
        )

    try:
        # Check if this is a GCS reference
        if isinstance(data, bytes_t) and data.startswith(GCS_REF_TYPE):
            # Extract reference JSON
            reference_json = data[len(GCS_REF_TYPE):]
            reference = json.loads(reference_json.decode('utf-8'))

            # Validate reference structure
            if reference.get('type') != 'gcs_ref' or 'uri' not in reference:
                raise DecodeError('Invalid GCS reference structure')

            # Download from GCS
            serialized_data = _gcs_backend.download(reference['uri'])
        else:
            # Direct payload
            serialized_data = data

        # Deserialize with cloudpickle
        return cloudpickle.loads(zstandard.decompress(serialized_data))

    except (DecodeError, SerializerNotInstalled):
        raise
    except Exception as exc:
        raise DecodeError(
            f'Failed to deserialize with cloudpickle: {exc}'
        )


def register_cloudpickle_gcs(registry):
    """Register cloudpickle-gcs serializer.

    This serializer uses cloudpickle for enhanced Python object serialization
    with automatic GCS offloading for large payloads.

    Args:
        registry: SerializerRegistry instance

    Note:
        This serializer is disabled by default for security reasons.
        Enable it with: registry.enable('cloudpickle_gcs')
    """
    try:
        import cloudpickle  # noqa
        from google.cloud import storage  # noqa

        registry.register(
            'cloudpickle_gcs',
            cloudpickle_gcs_dumps,
            cloudpickle_gcs_loads,
            content_type='application/x-cloudpickle-gcs',
            content_encoding='binary'
        )
    except ImportError:
        # Register with not_available handler
        def not_available(*args, **kwargs):
            raise SerializerNotInstalled(
                'cloudpickle-gcs serializer requires both cloudpickle '
                'and google-cloud-storage libraries. Install them with: '
                'pip install cloudpickle google-cloud-storage'
            )

        registry.register(
            'cloudpickle_gcs',
            None,
            not_available,
            content_type='application/x-cloudpickle-gcs'
        )