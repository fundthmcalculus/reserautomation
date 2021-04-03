import requests


class HttpConnectionBase:
    @staticmethod
    def _handle_response(response: requests.Response) -> None:
        if response.status_code >= 300:
            raise RuntimeError(f"Error {response}")
