#!/usr/bin/env python3


from typing import Any
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        """Initialize the counter and the que where we ingest the Data."""
        self.counter: int = 0
        self.que: list[tuple[int, str]] = []

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Mothermethode for validation"""
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Mothermethode for ingestion."""
        pass

    def output(self) -> tuple[int, str]:
        """Output the ingested data"""
        try:
            return self.que.pop(0)
        except IndexError:
            raise IndexError("Queue is empty.")


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        """Initialize the NumericProcessor Class."""
        super().__init__()

    def validate(self, data: Any) -> bool:
        """Validate the data for NumericProcessor."""
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return (all(isinstance(i, (int, float)) for i in data))
        else:
            return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        """Ingest the data for NumericProcessor."""
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, (int, float)):
            str_data: str = str(data)
            self.que.append((self.counter, str_data))
            self.counter += 1
        elif isinstance(data, list):
            for i in data:
                result: str = str(i)
                self.que.append((self.counter, result))
                self.counter += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        """Initialize the TextProcessor Class."""
        super().__init__()

    def validate(self, data: Any) -> bool:
        """Validate the data for TextProcessor."""
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return (all(isinstance(i, str) for i in data))
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        """Ingest the data for TextProcessor."""
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, str):
            self.que.append((self.counter, data))
            self.counter += 1
        elif isinstance(data, list):
            for i in data:
                self.que.append((self.counter, i))
                self.counter += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        """Initialize the LogProcessor Class."""
        super().__init__()

    def validate(self, data: Any) -> bool:
        """Validate the data for LogProcessor."""
        if isinstance(data, dict):
            return all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in data.items()
            )
        elif isinstance(data, list):
            return all(
                isinstance(i, dict) and all(
                    isinstance(k, str) and isinstance(v, str)
                    for k, v in i.items()
                )
                for i in data
            )
        else:
            return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        """Ingest the data for LogProcessor."""
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, dict):
            self.que.append((self.counter, (f"{data['log_level']}: "
                                            f"{data['log_message']}")))
            self.counter += 1
        elif isinstance(data, list):
            for i in data:
                self.que.append((self.counter, (f"{i['log_level']}: "
                                                f"{i['log_message']}")))
                self.counter += 1


def main() -> None:
    """Run the main Program."""
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    print(f" Trying to validate input '42': {numeric.validate(42)}")
    print(f" Trying to validate input 'Hello': {numeric.validate('Hello')}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest('foo')
    except ValueError as e:
        print(f" Got exception: {e}")
    dl_1: list[int | float] = [1, 2, 3, 4, 5]
    print(f" Processing data: {dl_1}")
    print(" Extracting 3 values...")
    numeric.ingest(dl_1)
    for i in range(0, 3):
        rang, wert = numeric.output()
        print(f" Numeric value {rang}: {wert}")

    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f" Trying to validate input '42': {text.validate(42)}")
    dl_2: list[str] = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {dl_2}")
    print(" Extracting 1 value...")
    text.ingest(dl_2)
    rang, wert = text.output()
    print(f" Text value {rang}: {wert}")

    print("\nTesting Log Processor...")
    log = LogProcessor()
    print(f" Trying to validate input 'Hello': {log.validate('Hello')}")
    dl_3: list[dict[str, str]] = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f" Processing data: {dl_3}")
    print(" Extracting 2 values...")
    log.ingest(dl_3)
    for i in range(0, 2):
        rang, wert = log.output()
        print(f" Log entry {rang}: {wert}")


if __name__ == "__main__":
    main()
