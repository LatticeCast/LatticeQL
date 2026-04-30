class LatticeQLError(Exception):
    pass


class LexError(LatticeQLError):
    pass


class ParseError(LatticeQLError):
    pass


class SchemaError(LatticeQLError):
    pass


class CodegenError(LatticeQLError):
    pass
