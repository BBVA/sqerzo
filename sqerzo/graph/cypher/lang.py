
# -------------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------------
from typing import List

from ...config import SQErzoConfig

def scape_string(text: str):
    return text.replace("""\\""", """\\\\""").replace("'", """\\'""")

def prepare_params(values: dict, operation="insert", node_name="a") -> List[str]:
    """Operation values: [insert|query|update]"""

    if not values:
        return []

    if operation == "insert":
        op = ":"
        nn = ""
    else:
        nn = f"{node_name}."
        op = ":"

    ret = []
    ret_append = ret.append

    for k, v in values.items():
        type_v = v.__class__.__name__

        if type_v == "str" and type_v in SQErzoConfig.SUPPORTED_TYPES:
            o = f"{nn}{k}{op} '{scape_string(v)}'"

        elif type_v == "datetime" and type_v in SQErzoConfig.SUPPORTED_TYPES:
            tm = v.strftime("%Y-%m-%dT%H:%M:%S%z")
            o = f"{nn}{k}{op} datetime('{tm}')"

        elif type_v == "bool" and type_v in SQErzoConfig.SUPPORTED_TYPES:
            o = f"{nn}{k}{op} {'true' if v else 'false'}"

        elif type_v == ("int", "float") and type_v in SQErzoConfig.SUPPORTED_TYPES:
            o = f"{nn}{k}{op} {v}"
        else:
            o = f"{nn}{k}{op} '{scape_string(str(v))}'"

        ret_append(o)

    return ret



def create_query(node, partial: bool = False):

    if not node.identity:
        node.identity = node.make_identity()

        # Do not include in dirty properties
        del node.__dirty_properties__["identity"]

    labels = node.labels()

    tmp_prop = [f"identity: '{node.identity}'"]
    tmp_prop.extend(prepare_params(node.properties))
    tmp_prop.extend(prepare_params({
        k: v
        for k, v in node.__dict__.items()
        if
        k not in ("properties", "identity") and not k.startswith("_")
    }))

    prop = f"{{{', '.join(tmp_prop)}}}"

    return f"{'' if partial else 'CREATE '} (:{labels} {prop})"

__all__ = ("create_query", )
