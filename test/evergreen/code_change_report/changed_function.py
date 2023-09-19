class ChangedFunction:
    def __init__(self,
                 new_file_path: str,
                 function_name: str,
                 code_complexity: int):
        self.new_file_path = new_file_path
        self.function_name = function_name
        self.code_complexity = code_complexity
