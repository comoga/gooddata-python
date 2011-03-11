

def to_identifier(text):
    if not text:
        return ''
    # TODO: more complex in StringUtil.java:79 convertToIdentifier
    return text.lower()

def to_title(text):
    if not text:
        return ''
    # TODO: more complex in StringUtil.java:70 toTitle
    return text.strip()