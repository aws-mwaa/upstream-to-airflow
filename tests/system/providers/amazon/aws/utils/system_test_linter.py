import os
import warnings
from pprint import pformat
from typing import Dict, List, Set

# Sample Output:
"""
System Test doctags contain forbidden parameters:
{'example_batch.py': {'wait_for_completion': [207]},
 'example_datasync.py': {'wait_for_completion': [149, 163, 189]},
 'example_dms.py': {'trigger_rule': [378]},
 'example_eks_with_nodegroup_in_one_step.py': {'trigger_rule': [97]},
 'example_emr_serverless.py': {'wait_for_completion': [74]},
 'example_glue.py': {'wait_for_completion': [149, 169]},
 'example_quicksight.py': {'wait_for_completion': [180]},
 'example_rds_export.py': {'wait_for_completion': [119]},
 'example_redshift_cluster.py': {'trigger_rule': [116],
                                 'wait_for_completion': [100]},
 'example_sagemaker.py': {'trigger_rule': [486],
                          'wait_for_completion': [432, 455, 471]},
 'example_sagemaker_endpoint.py': {'wait_for_completion': [234]}}
"""

# These are the parameters we do not want to see in the code snippets
FORBIDDEN_PARAMETERS: Set[str] = {'trigger_rule', 'wait_for_completion'}
# Any file starting with one of these prefixes will be checked
SYSTEM_TEST_FILENAME_PREFIXES: Set[str] = {'example_'}

# Relative path to the system tests
SYSTEM_TEST_DIR: str = '../'
# Code snippets start with:
DOCTAG_START: str = '# [START'
# Code snippets end with:
DOCTAG_END: str = '# [END'
# Appending this to the end of the line will not mark it as a violation
ABORT_CODE: str = 'noqa: clean_snippet'


def snippet_linter():
    def _add_violation(_filename, _parameter, _line_number):
        # If this is the first violation in this file, add the file to the dict.
        if not violations.get(_filename):
            violations[_filename] = {}
        # If this is the first time this parameter is found in this file, add the parameter to the dict.
        if not violations[_filename].get(_parameter):
            violations[_filename][_parameter] = []
        # Add the offending line number to the dict for this parameter in this file.
        violations[_filename][_parameter].append(_line_number)

    def _is_system_test(_filename):
        return any(prefix in _filename for prefix in SYSTEM_TEST_FILENAME_PREFIXES)

    def _get_system_tests(path):
        return [_filename for _filename in os.listdir(path) if _is_system_test(_filename)]

    violations: Dict[str, Dict[str, List[int]]] = {}  # What a ridiculous type declaration

    for filename in _get_system_tests(SYSTEM_TEST_DIR):
        file_path: str = os.path.join(SYSTEM_TEST_DIR, filename)
        tracking: bool = False
        with open(file_path, 'r') as file:
            # For each line, only pay attention to the lines between the doctag_start and doctag_end.
            for line_num, line_text in enumerate(file, 1):
                if not tracking:
                    if DOCTAG_START in line_text:
                        tracking = True
                elif DOCTAG_END in line_text:
                    tracking = False
                else:
                    # For those lines, log any forbidden parameters found and the
                    # line number it is on unless it also contains the ABORT_CODE.
                    for parameter in FORBIDDEN_PARAMETERS:
                        if parameter in line_text and ABORT_CODE not in line_text:
                            _add_violation(filename, parameter, line_num)
    return violations


if __name__ == '__main__':
    violation_list = snippet_linter()
    if violation_list:
        warnings.warn(f'\nSystem Test doctags contain forbidden parameters:\n{pformat(violation_list)}\n')
