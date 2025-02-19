from typing import List

class APIGenerationError(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(self.message)

class AreaGenerationError(Exception):
    def __init__(self, area_name: str, message: str) -> None:
        self.message = f"Could not create the area {area_name}: " + message
        super().__init__(self.message)

class LinkGenerationError(Exception):
    def __init__(self, area_from: str, area_to: str, message: str) -> None:
        self.message = f"Could not create the link {area_from} / {area_to}: " + message
        super().__init__(self.message)
