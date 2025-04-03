"""Yaml Serialization Utilities."""

from typing import Type

from yaml import SafeDumper


def register_yaml_decoder(type_: Type):
    r"""Register class known to deserialize themselves as yaml.

    It expect the __yaml__ method to be implemented on the instances
    of the given type.

    Arguments:
        type_ (Type): The type of the data to be serialized.

    Returns:
        Type: the type_ argument.

    Examples:
        >>> from kombu.utils.yaml import register_yaml_decoders
        >>> from kombu.serialization import dumps


        >>> @register_yaml_decoder
        ... class Custom:
        ...     def __init__(self, a):
        ...         self.a = a
        ...     def __yaml__(self):
        ...         return {'a': self.a}
        ...
        >>>

        >>> dumps({'my_obj': Custom(a=1)}, serializer='yaml')
        ('application/x-yaml', 'utf-8', 'my_obj:\n  a: 1\n')
    """
    assert hasattr(type_, '__yaml__')
    SafeDumper.add_representer(type_, represent__yaml__)
    return type_


def represent__yaml__(self, data):
    """Represent any object implementing __yaml__().

    See Also:
        https://pyyaml.org/wiki/PyYAMLDocumentation .

    Arguments:
        self (yaml.Dumper): dumper being executed.

        data (Any): data being serializer into yaml.
            We expect to find a method __yaml__ on this object.
    """
    return self.represent_dict(data.__yaml__())
