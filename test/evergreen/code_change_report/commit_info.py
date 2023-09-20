from change_info import ChangeInfo
from changed_function import ChangedFunction


class CommitInfo:
    def __init__(self,
                 short_id: str,
                 long_id: str,
                 message: str):
        self.short_id = short_id
        self.long_id = long_id
        self.message = message
        self.changes = []

    def add_change(self, change: ChangeInfo):
        self.changes.append(change)

    def get_changed_functions(self):
        changed_functions = {}

        for change in self.changes:
            changed_function = ChangedFunction(new_file_path=change.new_file_path,
                                               function_name=change.function_name,
                                               code_complexity=change.code_complexity)
            changed_functions[changed_function.function_name] = changed_function

        return changed_functions
