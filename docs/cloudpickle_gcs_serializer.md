# CloudPickle GCS Serializer

## Overview

The `cloudpickle_gcs` serializer is a powerful serialization backend for Kombu that combines the flexibility of CloudPickle with Google Cloud Storage (GCS) for handling large payloads.

### Key Features

- **Automatic size-based routing**: Small payloads (< threshold) are serialized inline, large payloads are offloaded to GCS
- **Enhanced Python object support**: Uses CloudPickle to serialize complex objects including lambdas, nested functions, and more
- **Transparent operation**: Encoding/decoding happens automatically with no changes to your application code
- **Lifecycle management**: Automatic cleanup of GCS objects via TTL policies
- **Configurable**: All parameters configurable via environment variables

## Installation

### Dependencies

```bash
pip install cloudpickle google-cloud-storage zstandard
```

### GCS Setup

1. **Create a GCS bucket**:
   ```bash
   gsutil mb gs://kombu-large-payloads
   ```

2. **Configure lifecycle policy** (optional but recommended):
   ```bash
   # Create lifecycle.json
   cat > lifecycle.json <<EOF
   {
     "lifecycle": {
       "rule": [
         {
           "action": {"type": "Delete"},
           "condition": {
             "daysSinceCustomTime": 7
           }
         }
       ]
     }
   }
   EOF

   # Apply lifecycle policy
   gsutil lifecycle set lifecycle.json gs://kombu-large-payloads
   ```

3. **Set up authentication**:

   **Option A: Service Account** (recommended for production)
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

   **Option B: Default Credentials** (for local development)
   ```bash
   gcloud auth application-default login
   ```

## Configuration

Configure the serializer via environment variables:

```bash
# Size threshold (in bytes) - payloads larger than this go to GCS
# Default: 10485760 (10MB)
export CLOUDPICKLE_GCS_THRESHOLD=10485760

# GCS bucket name
# Default: 'kombu-large-payloads'
export CLOUDPICKLE_GCS_BUCKET=your-bucket-name

# Object key prefix
# Default: 'kombu/'
export CLOUDPICKLE_GCS_PREFIX=kombu/

# TTL in days for auto-cleanup
# Default: 7
export CLOUDPICKLE_GCS_TTL_DAYS=7
```

## Usage

### Enabling the Serializer

**IMPORTANT**: The `cloudpickle_gcs` serializer is disabled by default for security reasons (like `pickle` and `yaml`). You must explicitly enable it:

```python
from kombu.serialization import registry

# Enable cloudpickle_gcs serializer
registry.enable('cloudpickle_gcs')
```

Or use the convenience function:

```python
from kombu.serialization import enable_insecure_serializers

# Enable cloudpickle_gcs along with other insecure serializers
enable_insecure_serializers(['cloudpickle_gcs'])
```

### Basic Usage

```python
from kombu import Connection, Exchange, Queue

# Enable the serializer
from kombu.serialization import registry
registry.enable('cloudpickle_gcs')

# Create connection and queue
conn = Connection('amqp://guest@localhost//')
exchange = Exchange('test_exchange', type='direct')
queue = Queue('test_queue', exchange=exchange, routing_key='test')

# Publish with cloudpickle_gcs serializer
with conn.Producer(serializer='cloudpickle_gcs') as producer:
    # Small payload - will be inline
    small_data = {'message': 'Hello, World!'}
    producer.publish(
        small_data,
        exchange=exchange,
        routing_key='test',
        declare=[queue]
    )

    # Large payload - will be offloaded to GCS
    large_data = {
        'data': 'x' * (15 * 1024 * 1024),  # 15MB
        'metadata': {'timestamp': datetime.now()}
    }
    producer.publish(
        large_data,
        exchange=exchange,
        routing_key='test'
    )

# Consume messages
with conn.Consumer(queue, callbacks=[process_message], accept=['application/x-cloudpickle-gcs']) as consumer:
    consumer.consume()
    conn.drain_events()

def process_message(body, message):
    print(f"Received: {body}")
    message.ack()
```

### Advanced Usage: Complex Objects

CloudPickle can serialize complex Python objects that standard pickle cannot:

```python
from kombu.serialization import dumps, loads, registry

# Enable the serializer
registry.enable('cloudpickle_gcs')

# Serialize lambda functions
data = {
    'transform': lambda x: x * 2,
    'filter': lambda x: x > 0
}

content_type, content_encoding, payload = dumps(data, serializer='cloudpickle_gcs')
result = loads(payload, content_type, content_encoding, force=True)

print(result['transform'](5))  # Output: 10
```

### Custom Threshold for Specific Messages

```python
from kombu.serialization_cloudpickle_gcs import cloudpickle_gcs_dumps

# Use custom 5MB threshold for this specific message
data = {'large': 'x' * 6000000}  # 6MB
serialized = cloudpickle_gcs_dumps(data, threshold=5*1024*1024)
```

## Architecture

### How It Works

1. **Encoding (Producer Side)**:
   ```
   Object → cloudpickle.dumps() → Check size
                                      ↓
                    ┌─────────────────┴──────────────────┐
                    ↓                                     ↓
            Size < Threshold                     Size >= Threshold
                    ↓                                     ↓
            Return serialized data            Upload to GCS
                                                          ↓
                                              Return GCS reference
   ```

2. **Decoding (Consumer Side)**:
   ```
   Payload → Check if GCS reference
                    ↓
       ┌────────────┴────────────┐
       ↓                         ↓
   GCS Reference           Direct Payload
       ↓                         ↓
   Download from GCS      Use payload as-is
       ↓                         ↓
       └────────────┬────────────┘
                    ↓
         cloudpickle.loads()
                    ↓
           Return Python object
   ```

### GCS Reference Format

When a payload is offloaded to GCS, a JSON reference is sent instead:

```json
{
  "type": "gcs_ref",
  "uri": "gs://bucket/kombu/20250109/abc123def456.cloudpickle",
  "size": 15728640,
  "key": "kombu/20250109/abc123def456.cloudpickle"
}
```

### Object Naming Convention

GCS objects are stored with the following key format:
```
{prefix}/{YYYYMMDD}/{uuid}.cloudpickle
```

Example:
```
kombu/20250109/7f3b2e1a4c9d8e5f.cloudpickle
```

## IAM Permissions

### Minimum Required Permissions

Your service account needs the following permissions on the GCS bucket:

```json
{
  "bindings": [
    {
      "role": "roles/storage.objectCreator",
      "members": ["serviceAccount:your-service-account@project.iam.gserviceaccount.com"]
    },
    {
      "role": "roles/storage.objectViewer",
      "members": ["serviceAccount:your-service-account@project.iam.gserviceaccount.com"]
    }
  ]
}
```

Or use predefined roles:
- `roles/storage.objectUser` (read and write)

### Setting Permissions

```bash
# Grant permissions to service account
gsutil iam ch \
  serviceAccount:your-service-account@project.iam.gserviceaccount.com:objectAdmin \
  gs://kombu-large-payloads
```

## Performance Considerations

### Latency

- **Small payloads (< threshold)**: No additional latency, standard cloudpickle serialization
- **Large payloads (>= threshold)**:
  - Encoding: +50-200ms (GCS upload time, varies by payload size and network)
  - Decoding: +50-200ms (GCS download time)

### Cost

GCS costs include:
- **Storage**: ~$0.020/GB/month (Standard storage)
- **Operations**:
  - Write: $0.05 per 10,000 operations
  - Read: $0.004 per 10,000 operations
- **Network egress**: Variable based on location

**Example monthly cost** for 1000 messages/day with 15MB average payload:
- Storage (7-day TTL): ~$0.06/month
- Operations: ~$0.30/month
- Total: ~$0.36/month

### Optimization Tips

1. **Tune the threshold**: Set it based on your network latency and message patterns
   ```bash
   # For fast networks, you might increase the threshold
   export CLOUDPICKLE_GCS_THRESHOLD=52428800  # 50MB
   ```

2. **Use regional buckets**: Place the GCS bucket in the same region as your workers
   ```bash
   gsutil mb -l us-central1 gs://kombu-large-payloads
   ```

3. **Enable bucket versioning** (optional): For data recovery
   ```bash
   gsutil versioning set on gs://kombu-large-payloads
   ```

## Error Handling

### Common Errors

1. **SerializerNotInstalled**: Missing dependencies
   ```python
   # Install required packages
   pip install cloudpickle google-cloud-storage
   ```

2. **EncodeError: GCS bucket does not exist**:
   ```bash
   # Create the bucket
   gsutil mb gs://your-bucket-name
   ```

3. **Authentication errors**:
   ```bash
   # Check credentials
   gcloud auth application-default login
   # Or set service account key
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"
   ```

4. **ContentDisallowed**: Serializer is disabled
   ```python
   # Enable the serializer
   from kombu.serialization import registry
   registry.enable('cloudpickle_gcs')
   ```

## Security Considerations

### Security Model

1. **Disabled by default**: Like `pickle` and `yaml`, this serializer is disabled by default
2. **Pickle security**: CloudPickle has the same security concerns as pickle - arbitrary code execution
3. **Use only in trusted environments**: Only deserialize messages from trusted sources
4. **GCS access control**: Use IAM to restrict bucket access

### Best Practices

1. **Enable only when needed**:
   ```python
   # Enable only for specific consumers that need it
   registry.enable('cloudpickle_gcs')
   ```

2. **Use accept parameter**:
   ```python
   # Explicitly accept only trusted content types
   loads(data, content_type, content_encoding,
         accept=['application/x-cloudpickle-gcs'])
   ```

3. **Encrypt at rest**: Enable GCS encryption
   ```bash
   # Encryption is enabled by default in GCS
   # For customer-managed keys:
   gsutil kms encryption -k projects/PROJECT/locations/LOCATION/keyRings/RING/cryptoKeys/KEY \
     gs://kombu-large-payloads
   ```

4. **Network security**: Use VPC Service Controls or Private Google Access

## Monitoring and Debugging

### Logging

Enable logging to track serialization behavior:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('kombu.serialization_cloudpickle_gcs')
```

### Metrics to Monitor

1. **GCS operations**:
   - Upload/download success rate
   - Latency percentiles (p50, p95, p99)
   - Payload size distribution

2. **Message processing**:
   - Serialization errors
   - GCS reference vs inline ratio
   - TTL cleanup effectiveness

### Debugging Tips

```python
# Check if message was offloaded to GCS
from kombu.serialization_cloudpickle_gcs import GCS_REF_TYPE

if payload.startswith(GCS_REF_TYPE):
    print("Message offloaded to GCS")
    import json
    ref = json.loads(payload[len(GCS_REF_TYPE):].decode('utf-8'))
    print(f"GCS URI: {ref['uri']}")
    print(f"Original size: {ref['size']} bytes")
else:
    print("Message inline")
```

## Migration Guide

### From Standard Pickle

```python
# Before
producer.publish(data, serializer='pickle')

# After
from kombu.serialization import registry
registry.enable('cloudpickle_gcs')
producer.publish(data, serializer='cloudpickle_gcs')
```

### Gradual Migration

Support both serializers during migration:

```python
# Producer: Send with new serializer
registry.enable('cloudpickle_gcs')
producer.publish(data, serializer='cloudpickle_gcs')

# Consumer: Accept both
consumer = Consumer(
    queue,
    callbacks=[process_message],
    accept=['application/x-python-serialize', 'application/x-cloudpickle-gcs']
)
```

## Troubleshooting

### Issue: Messages not being consumed

**Cause**: Serializer disabled or not in accept list

**Solution**:
```python
# Enable serializer
registry.enable('cloudpickle_gcs')

# Add to accept list
consumer = Consumer(queue, accept=['application/x-cloudpickle-gcs'])
```

### Issue: High GCS costs

**Cause**: Threshold too low, too many small payloads going to GCS

**Solution**: Increase threshold
```bash
export CLOUDPICKLE_GCS_THRESHOLD=52428800  # 50MB
```

### Issue: Slow message processing

**Cause**: GCS upload/download latency

**Solutions**:
1. Use regional bucket in same region as workers
2. Increase threshold to reduce GCS operations
3. Consider using GCS Transfer Service for bulk operations

## Testing

Run the test suite:

```bash
# Run all serialization tests
pytest kombu/tests/test_serialization_cloudpickle_gcs.py -v

# Run specific test
pytest kombu/tests/test_serialization_cloudpickle_gcs.py::test_CloudpickleGCSSerialization::test_large_payload_roundtrip -v
```

## Limitations

1. **GCS dependency**: Requires Google Cloud Storage (not portable to other cloud providers without modification)
2. **Pickle security**: Inherits pickle's security limitations
3. **Network dependency**: Requires network access to GCS for large payloads
4. **Cost**: GCS operations incur costs (though minimal for most use cases)
5. **Latency**: Additional latency for large payloads due to GCS round-trip

## Future Enhancements

Potential improvements for future versions:

1. **Multi-cloud support**: Abstract storage backend for S3, Azure Blob, etc.
2. **Compression**: Optional compression before upload
3. **Caching**: Local cache for frequently accessed GCS objects
4. **Async operations**: Non-blocking GCS uploads/downloads
5. **Metrics integration**: Built-in Prometheus metrics
6. **Encryption**: Application-level encryption before GCS upload

## Support

For issues, questions, or contributions:
- GitHub Issues: https://github.com/celery/kombu/issues
- Documentation: https://kombu.readthedocs.io/

## License

This serializer is part of Kombu and is released under the same license (BSD).