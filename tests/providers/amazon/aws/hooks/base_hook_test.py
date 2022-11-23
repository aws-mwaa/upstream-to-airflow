from unittest import mock

import pytest

from airflow.providers_manager import ProvidersManager


def _conn_path():
    hook_class_name = ProvidersManager().hooks['aws'].hook_class_name
    hook_path = '.'.join(hook_class_name.split('.')[:-2])
    return hook_path


class BaseHookUnitTest:
    @pytest.fixture
    def mock_conn(self, request):
        service, hook_name = request.param
        mock_path = f'{_conn_path()}.{service.lower()}.{hook_name}.conn'
        with mock.patch(mock_path, new_callable=mock.PropertyMock) as _mock_conn:
            yield _mock_conn
