_aliases = {}
_encoders = {}
_decoders = {}


def register(encoder, decoder, content_type, aliases=[]):
    _encoders[content_type] = encoder
    _decoders[content_type] = decoder
    _aliases.update((alias, content_type) for alias in aliases)


def get_encoder(t):
    t = _aliases.get(t, t)
    return _encoders[t], t


def get_decoder(t):
    return _decoders[_aliases.get(t, t)]


def compress(body, content_type):
    encoder, content_type = get_encoder(content_type)
    return encoder(body), content_type


def decompress(body, content_type):
    return get_decoder(content_type)(body)


register(lambda x: x.encode("zlib"),
         lambda x: x.decode("zlib"),
         "application/x-gzip", aliases=["gzip", "zlib"])
register(lambda x: x.encode("bz2"),
         lambda x: x.decode("bz2"),
         "application/x-bz2", aliases=["bzip2", "bzip"])
