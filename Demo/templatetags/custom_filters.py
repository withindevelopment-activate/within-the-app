from django import template
from django.contrib import messages

register = template.Library()

@register.filter
def dict_key(value, key):
    return value.get(key, None)

@register.filter
def map_kpi(data_list, key):
    return [item.get(key, None) for item in data_list]

@register.filter
def replace_underscores(value):
    return value.replace("_", " ")

@register.filter
def dict_keys(d):
    return list(d.keys()) if isinstance(d, dict) else []

@register.filter
def dict_values(d):
    return list(d.values()) if isinstance(d, dict) else []

@register.filter
def get_item(dictionary, key):
    return dictionary.get(key, "")

@register.filter
def map(value, arg):
    """Map filter: extracts the item at index arg from iterable of tuples."""
    try:
        return [v[int(arg)] for v in value]
    except Exception as e:
            messages.error(f"Error: {str(e)}")

@register.filter
def unique(value):
    """Removes duplicates while preserving order."""
    seen = set()
    return [x for x in value if not (x in seen or seen.add(x))]