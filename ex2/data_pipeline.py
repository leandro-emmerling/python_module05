#!/usr/bin/env python3


from typing import Any, Protocol
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        """Initialize the counter and the que where we ingest the Data."""
        self.counter: int = 0
        self.que: list[tuple[int, str]] = []
        self.name: str = ""

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
        self.name: str = "Numeric Processor"

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
        self.name: str = "Text Processor"

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
        self.name: str = "Log Processor"

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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int,str]]) -> None:
        ...


class DataStream():
    def __init__(self) -> None:
        """Initialize the DataStream Class."""
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Register Processor to processors list"""
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        """Analyze and send each element to the registered data processor
        or print an Error"""
        for data in stream:
            found: bool = False
            for proc in self.processors:
                if proc.validate(data) is True:
                    proc.ingest(data)
                    found = True
                    break
            if not found:
                print(f"DataStream error - "
                      f"Can't process element in stream: {data}")

    def print_processors_stats(self) -> None:
        """Prints the statistics of the processed stream"""
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
        else:
            for proc in self.processors:
                print(f"{proc.name}: total {proc.counter} items processed, "
                      f"remaining {len(proc.que)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        pass


def main() -> None:
    """Run the main Program."""
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    print("\nRegistering Processors\n")
    stream.register_processor(numeric)
    stream.register_processor(text)
    stream.register_processor(log)
    dl_1: list[Any] = [
        'Hello world', [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message':
            'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42, ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {dl_1}")
    stream.process_stream(dl_1)
    stream.print_processors_stats()
    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        numeric.output()
    for _ in range(2):
        text.output()
    log.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
