"""
Module containing shared code of Batch Process API and Batch Statistical API
"""
from abc import ABCMeta
from enum import Enum
from typing import Generic, Iterable, Optional, Sequence, Type, TypeVar, Union

from ...constants import RequestType
from ...types import Json, JsonDict
from ..base import SentinelHubService

BatchRequestType = TypeVar("BatchRequestType", bound="BaseBatchRequest")  # pylint: disable=invalid-name
RequestSpec = Union[str, dict, BatchRequestType]
Self = TypeVar("Self")


class BatchRequestStatus(Enum):
    """An enum class with all possible batch request statuses"""

    CREATED = "CREATED"
    ANALYSING = "ANALYSING"
    ANALYSIS_DONE = "ANALYSIS_DONE"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    CANCELED = "CANCELED"


class BatchUserAction(Enum):
    """An enum class with all possible batch user actions"""

    START = "START"
    ANALYSE = "ANALYSE"
    NONE = "NONE"
    CANCEL = "CANCEL"


class BaseBatchClient(SentinelHubService, Generic[BatchRequestType], metaclass=ABCMeta):  # noqa: B024
    """Class containing common methods and helper functions for Batch Client classes"""

    def _call_job(self, batch_request: RequestSpec, endpoint_name: str) -> Json:
        """Makes a POST request to the service that triggers a processing job"""
        request_id = self._parse_request_id(batch_request)
        job_url = f"{self._get_processing_url(request_id)}/{endpoint_name}"
        return self.client.get_json(url=job_url, request_type=RequestType.POST, use_session=True)

    def _get_processing_url(self, request_id: Optional[str] = None) -> str:
        """Creates a URL for the batch statistical endpoint"""
        url = self.service_url
        if request_id is None:
            return url
        return f"{url}/{request_id}"

    @staticmethod
    def _parse_request_id(data: RequestSpec) -> str:
        """Parses batch request id from multiple possible inputs"""
        if isinstance(data, BaseBatchRequest):
            return data.request_id
        if isinstance(data, dict):
            return data["id"]
        if isinstance(data, str):
            return data
        raise ValueError(f"Expected a BatchRequest, dictionary or a string, got {data}.")


class BaseBatchRequest:
    """Class containing helper functions for Batch Request classes"""

    _REPR_PARAM_NAMES: Sequence[str]

    request_id: str
    error: Optional[str]
    status: BatchRequestStatus

    def to_dict(self) -> JsonDict:
        """Transforms itself into a dictionary form."""
        raise NotImplementedError("Method should be implemented or provided via `dataclass_json` decorator.")

    @classmethod
    def from_dict(cls: Type[Self], json_dict: JsonDict) -> Self:
        """Transforms itself into a dictionary form."""
        raise NotImplementedError("Method should be implemented or provided via `dataclass_json` decorator.")

    def __repr__(self) -> str:
        """A representation that shows the basic parameters of a batch job"""
        repr_params = {name: getattr(self, name) for name in self._REPR_PARAM_NAMES if getattr(self, name) is not None}
        repr_params_str = "\n  ".join(f"{name}={value}" for name, value in repr_params.items())
        return f"{self.__class__.__name__}(\n  {repr_params_str}\n  ...\n)"

    def raise_for_status(
        self,
        status: Union[str, BatchRequestStatus, Iterable[Union[str, BatchRequestStatus]]] = BatchRequestStatus.FAILED,
    ) -> None:
        """Raises an error in case batch request has a given status

        :param status: One or more status codes on which to raise an error. The default is `'FAILED'`.
        :raises: RuntimeError
        """
        if isinstance(status, (str, BatchRequestStatus)):
            status = [status]
        status_list = [BatchRequestStatus(_status) for _status in status]

        if self.status in status_list:
            formatted_error_message = f' and error message: "{self.error}"' if self.error else ""
            raise RuntimeError(
                f"Raised for batch request {self.request_id} with status {self.status.value}{formatted_error_message}"
            )
