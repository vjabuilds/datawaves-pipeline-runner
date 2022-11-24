class SerializationNotSupported(Exception):
    def __init__(self, cls: type):
        super().__init__(f"The class {cls.__name__} does not support serialization")