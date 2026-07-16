from django import template

register = template.Library()

@register.filter(name='replace')
def replace(value, arg):
    """
    Replaces a substring with another.
    Usage: {{ value|replace:"old,new" }}
    """
    if isinstance(arg, str) and ',' in arg:
        old, new = arg.split(',', 1)
        return str(value).replace(old, new)
    return value

@register.filter(name='get_item')
def get_item(dictionary, key):
    """
    Allows accessing dictionary items with a variable key in templates.
    Usage: {{ my_dict|get_item:my_key }}
    """
    return dictionary.get(key)
