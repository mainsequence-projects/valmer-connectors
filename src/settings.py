from mainsequence.client import Constant as _C

PROJECT_BUCKET_NAME = "Data Connectors"
BUCKET_NAME_HISTORICAL_VECTORS = "Hitorical Valmer Vector Analytico"
SUBYACENTE_TO_INDEX_MAP = {
    "TIIE28": _C.get_value(name="REFERENCE_RATE__TIIE_28"),
    "TIIE182": _C.get_value(name="REFERENCE_RATE__TIIE_182"),
    "TIIE91": _C.get_value(name="REFERENCE_RATE__TIIE_91"),
    "TIIE28 EQUIV 182": _C.get_value(name="REFERENCE_RATE__TIIE_182"),
    "Tasa TIIE Fondeo 1D": _C.get_value(name="REFERENCE_RATE__TIIE_OVERNIGHT"),
    "CETE_28": _C.get_value(name="REFERENCE_RATE__CETE_28"),
    "CETE28": _C.get_value(name="REFERENCE_RATE__CETE_28"),
    "CETE182": _C.get_value(name="REFERENCE_RATE__CETE_182"),
    "Bonos M Bruta(Yield)": _C.get_value(name="REFERENCE_RATE__CETE_28"),
    "Fondeo Bancario": _C.get_value(name="REFERENCE_RATE__TIIE_OVERNIGHT"),
    "Tasa TIIE Fondeo 1D": _C.get_value(name="REFERENCE_RATE__TIIE_OVERNIGHT"),
    "IRMXP-FGub-28": _C.get_value(name="REFERENCE_RATE__CETE_28"),
    "IRMXP-FGub-91": _C.get_value(name="REFERENCE_RATE__CETE_91"),
    "AAA": _C.get_value(name="REFERENCE_RATE__TIIE_28"),
    "D1": _C.get_value(name="REFERENCE_RATE__TIIE_28"),
    "P8-X8": _C.get_value(name="REFERENCE_RATE__CETE_182"),
    "P12-X12": _C.get_value(name="REFERENCE_RATE__CETE_182"),
    "P4-X4": _C.get_value(name="REFERENCE_RATE__CETE_91"),
}
