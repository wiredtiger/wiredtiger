def is_useful_line(content: str) -> bool:
    useful_line = content != '\n' and content.strip() != '{' and content.strip() != '}'
    return useful_line
