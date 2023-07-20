from abc import ABC, abstractmethod

class IDownloader(ABC):

    @abstractmethod
    def download(self) -> None:
        """
            All downloader must have download method.
        """