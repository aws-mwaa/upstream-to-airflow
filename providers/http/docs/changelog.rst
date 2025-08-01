 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


.. NOTE TO CONTRIBUTORS:
   Please, only add notes to the Changelog just below the "Changelog" header when there are some breaking changes
   and you want to add an explanation to the users on how they are supposed to deal with them.
   The changelog is updated and maintained semi-automatically by release manager.

``apache-airflow-providers-http``


Changelog
---------

5.3.3
.....

Misc
~~~~

* ``Cleanup type ignores (#53294)``
* ``Remove type ignore across codebase after mypy upgrade (#53243)``
* ``Make http provider compatible with mypy 1.16.1 (#53108)``
* ``Remove upper-binding for "python-requires" (#52980)``
* ``Temporarily switch to use >=,< pattern instead of '~=' (#52967)``
* ``Move all BaseHook usages to version_compat in HTTP (#52814)``
* ``Add Python 3.13 support for Airflow. (#46891)``
* ``Bump aiohttp to 3.12.14 (#53830)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Resolve timeout in ''HttpSensor'' test by fixing template rendering (#53676)``

5.3.2
.....

Bug Fixes
~~~~~~~~~

* ``Ensure  'HttpHook.run()' does not alter 'extra_options' passed to it (#51893)``
* ``Fix HttpOperator(deferrable=True) crash when connection has login / password (#52050)``
* ``Enable extra_options even when extra not defined for HttpHook.run (#51746)``

Misc
~~~~

* ``Move 'BaseHook' implementation to task SDK (#51873)``
* ``Provider Migration: Replace 'BaseOperator' to Task SDK for 'apache/http' (#52528)``
* ``Drop support for Python 3.9 (#52072)``
* ``Use BaseSensorOperator from task sdk in providers (#52296)``
* ``Add deprecation to 'airflow/sensors/base.py' (#52249)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Revert "Replace models.BaseOperator to Task SDK for http (#52506)" (#52515)``
   * ``Replace models.BaseOperator to Task SDK for http (#52506)``
   * ``Remove db usage from http provider tests (#52227)``

5.3.1
.....

Misc
~~~~

* ``Bump some provider dependencies for faster resolution (#51727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Increasing task timeout by 2 seconds for more resilient runs (#50578)``

5.3.0
.....

.. note::
    This release of provider is only available for Airflow 2.10+ as explained in the
    Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>_.

Misc
~~~~

* ``Bump min Airflow version in providers to 2.10 (#49843)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description of provider.yaml dependencies (#50231)``
   * ``Avoid committing history for providers (#49907)``

5.2.2
.....

Bug Fixes
~~~~~~~~~

* ``HttpHook: allways add port if given (#48573)``

Misc
~~~~

* ``remove superfluous else block (#49199)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Apr 2nd wave of providers (#49051)``
   * ``Remove unnecessary entries in get_provider_info and update the schema (#48849)``
   * ``Remove fab from preinstalled providers (#48457)``
   * ``Improve documentation building iteration (#48760)``
   * ``Prepare docs for Apr 1st wave of providers (#48828)``
   * ``Simplify tooling by switching completely to uv (#48223)``
   * ``Upgrade ruff to latest version (#48553)``
   * ``Move 'BaseSensorOperator' to TaskSDK definitions (#48244)``
   * ``Prepare docs for Mar 2nd wave of providers (#48383)``
   * ``Upgrade providers flit build requirements to 3.12.0 (#48362)``
   * ``Move airflow sources to airflow-core package (#47798)``
   * ``Remove links to x/twitter.com (#47801)``

5.2.1
.....

Bug Fixes
~~~~~~~~~

* ``Fix HttpSensor to enable returning xcom value (#46848)``

Misc
~~~~

* ``Upgrade flit to 3.11.0 (#46938)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move tests_common package to devel-common project (#47281)``
   * ``Improve documentation for updating provider dependencies (#47203)``
   * ``Add legacy namespace packages to airflow.providers (#47064)``
   * ``Remove extra whitespace in provider readme template (#46975)``

5.2.0
.....

.. note::
  This version has no code changes. It's released due to yank of previous version due to packaging issues.

5.1.0
.....

Features
~~~~~~~~

* ``Deferrable support for HttpOperator (#45228)``
* ``Allow check_response to be defined in extra_options of HTTP connection (#45451)``

Misc
~~~~

* ``AIP-72: Support better type-hinting for Context dict in SDK  (#45583)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Move provider_tests to unit folder in provider tests (#46800)``
   * ``Removed the unused provider's distribution (#46608)``
   * ``Fix doc issues found with recent moves (#46372)``
   * ``Fix few issues with constraints generation broken by moving providers (#46139)``

5.0.0
.....

.. note::
  This release of provider is only available for Airflow 2.9+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Breaking changes
~~~~~~~~~~~~~~~~

.. warning::
  All deprecated classes, parameters and features have been removed from the HTTP provider package.
  The following breaking changes were introduced:

  * Operators
     * Remove ``airflow.providers.http.operators.http.SimpleHttpOperator``. Use ``airflow.providers.http.operators.http.HttpOperator`` instead.

* ``Remove Provider Deprecations in HTTP (#44542)``

Features
~~~~~~~~

* ``[Providers/HTTP] Add adapter parameter to HttpHook to allow custom requests adapters (#44302)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.9.0 (#44956)``
* ``Update DAG example links in multiple providers documents (#44034)``
* ``Rename execution_date to logical_date across codebase (#43902)``
* ``Bump aiohttp to 3.11.1 (#44036)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Correct new changelog breaking changes header (#44659)``
   * ``Use Python 3.9 as target version for Ruff & Black rules (#44298)``

.. Review and move the new changes to one of the sections above:
   * ``Update path of example dags in docs (#45069)``

4.13.3
......

Bug Fixes
~~~~~~~~~

* ``Fix(http) bug, pass request_kwargs to HttpHooK.run (#43459)``

Misc
~~~~

* ``Limit temporarily aiohttp to < 3.11.0 (#44006)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.13.2
......

Bug Fixes
~~~~~~~~~

* ``fix: HttpSensorTrigger to include 'method' when serializing (#42925)``
* ``Use url_from_endpoint inside HttpHook. (#42785)``

Misc
~~~~

* ``add lowerbount to requests-toolbelt and replace requests_toolbelt with requests-toolbelt (#43020)``
* ``Set lower bound to asgiref>=2.3.0 (#43001)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Split providers out of the main "airflow/" tree into a UV workspace project (#42505)``

4.13.1
......

Misc
~~~~

* ``remove deprecated soft_fail from providers (#41727)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):

4.13.0
......

.. note::
  This release of provider is only available for Airflow 2.8+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.8.0 (#41396)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for Aug 1st wave of providers (#41230)``
   * ``Prepare docs 1st wave July 2024 (#40644)``
   * ``Enable enforcing pydocstyle rule D213 in ruff. (#40448)``

4.12.0
......

Features
~~~~~~~~

* ``Add 'retry_args' parameter to 'HttpOperator' (#40086)``

4.11.1
......

Misc
~~~~

* ``Faster 'airflow_version' imports (#39552)``
* ``Simplify 'airflow_version' imports (#39497)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Reapply templates for all providers (#39554)``

4.11.0
......

.. note::
  This release of provider is only available for Airflow 2.7+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Features
~~~~~~~~

* ``Allow trust env parameter to be defined in extra options of HTTP Connection (#39161)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.7.0 (#39240)``

4.10.1
......

Bug Fixes
~~~~~~~~~

* ``fix(http): add return statement to yield within a while loop in triggers (#38392)``
* ``Consolidate HttpOperator http request between sync and async mode (#37293)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update yanked versions in providers changelogs (#38262)``

4.10.0
......

Features
~~~~~~~~

* ``Change the URL building in HttpHookAsync to match the behavior of HttpHook (#37696)``

Misc
~~~~

* ``Return back removed public HttpHook's method (#37738)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add comment about versions updated by release manager (#37488)``

4.9.1
.....

Misc
~~~~

* ``Bump aiohttp min version to avoid CVE-2024-23829 and CVE-2024-23334 (#37110)``
* ``feat: Switch all class, functions, methods deprecations to decorators (#36876)``
* ``Get rid of pytest-httpx as dependency (#37334)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Added D401 support to http, smtp and sftp provider (#37303)``
   * ``Upgrade mypy to 1.8.0 (#36428)``

4.9.0
.....

Features
~~~~~~~~

* ``Allow default requests parameters like proxy to be defined in extra options field of a Airflow HTTP Connection (#36733)``
* ``Add deferrable implementation in HTTPSensor (#36904)``

Bug Fixes
~~~~~~~~~

* ``Fix stacklevel in warnings.warn into the providers (#36831)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 1st wave of Providers January 2024 (#36640)``
   * ``Speed up autocompletion of Breeze by simplifying provider state (#36499)``
   * ``Re-apply updated version numbers to 2nd wave of providers in December (#36380)``
   * ``Add documentation for 3rd wave of providers in Deember (#36464)``
   * ``Provide the logger_name param in providers hooks in order to override the logger name (#36675)``
   * ``Revert "Provide the logger_name param in providers hooks in order to override the logger name (#36675)" (#37015)``
   * ``Prepare docs 2nd wave of Providers January 2024 (#36945)``

4.8.0
.....

.. note::
  This release of provider is only available for Airflow 2.6+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``Fix HttpOperator pagination with 'str' data (#35782)``

Misc
~~~~

* ``Bump minimum Airflow version in providers to Airflow 2.6.0 (#36017)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix and reapply templates for provider documentation (#35686)``
   * ``Prepare docs 2nd wave of Providers November 2023 (#35836)``
   * ``Use reproducible builds for providers (#35693)``

4.7.0
.....

Features
~~~~~~~~

* ``Add pagination to 'HttpOperator' and make it more modular (#34669)``

Bug Fixes
~~~~~~~~~

* ``Fix json data for async PUTs (#35405)``
* ``Fix: Paginate on latest Response (#35560)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs 3rd wave of Providers October 2023 (#35187)``
   * ``Pre-upgrade 'ruff==0.0.292' changes in providers (#35053)``
   * ``Upgrade pre-commits (#35033)``
   * ``Prepare docs 3rd wave of Providers October 2023 - FIX (#35233)``
   * ``Prepare docs 1st wave of Providers November 2023 (#35537)``

4.6.0
.....

.. note::
  This release of provider is only available for Airflow 2.5+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Bug Fixes
~~~~~~~~~

* ``fix(providers/http): respect soft_fail argument when exception is raised (#34391)``

Misc
~~~~

* ``Bump min airflow version of providers (#34728)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Refactor usage of str() in providers (#34320)``

4.5.2
.....

Misc
~~~~

* ``Improve modules import in Airflow providers by some of them into a type-checking block (#33754)``
* ``Convert hard-coded allowlist error code to be argument of HttpSensor (#33717)``

4.5.1
.....

Misc
~~~~

* ``Refactor: Simplify code in smaller providers (#33234)``

4.5.0
.....

Features
~~~~~~~~

* ``Add deferrable mode to SimpleHttpOperator (#32448)``

Bug Fixes
~~~~~~~~~

* ``Fix headers passed into HttpAsyncHook (#32409)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Prepare docs for July 2023 wave of Providers (RC2) (#32381)``
   * ``Remove spurious headers for provider changelogs (#32373)``
   * ``Prepare docs for July 2023 wave of Providers (#32298)``
   * ``D205 Support - Providers: GRPC to Oracle (inclusive) (#32357)``
   * ``Improve provider documentation and README structure (#32125)``

4.4.2
.....

.. note::
  This release dropped support for Python 3.7

Misc
~~~~

* ``Add note about dropping Python 3.7 for providers (#32015)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Improve docstrings in providers (#31681)``
   * ``Add D400 pydocstyle check - Providers (#31427)``

4.4.1
.....

Misc
~~~~

* ``Bring back min-airflow-version for preinstalled providers (#31469)``

4.4.0 (YANKED)
..............

.. warning:: This release has been **yanked** with a reason: ``This version might cause unconstrained installation of old airflow version lead to Runtime Error.``

.. note::
  This release of provider is only available for Airflow 2.4+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Upgrade ruff to 0.0.262 (#30809)``
   * ``Add full automation for min Airflow version for providers (#30994)``
   * ``Add mechanism to suspend providers (#30422)``
   * ``Use '__version__' in providers not 'version' (#31393)``
   * ``Fixing circular import error in providers caused by airflow version check (#31379)``
   * ``Prepare docs for May 2023 wave of Providers (#31252)``

4.3.0
.....

Features
~~~~~~~~

* ``Add non login-password auth support for SimpleHttpOpeator (#29206)``

4.2.0
.....

Features
~~~~~~~~

* ``Add HttpHookAsync for deferrable implementation (#29038)``

4.1.1
.....

Misc
~~~~

* ``Change logging for HttpHook to debug (#28911)``

4.1.0
.....

.. note::
  This release of provider is only available for Airflow 2.3+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

Misc
~~~~

* ``Move min airflow version to 2.3.0 for all providers (#27196)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Enable string normalization in python formatting - providers (#27205)``
   * ``Update docs for September Provider's release (#26731)``
   * ``Apply PEP-563 (Postponed Evaluation of Annotations) to non-core airflow (#26289)``

4.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

The SimpleHTTPOperator, HttpSensor and HttpHook use now TCP_KEEPALIVE by default.
You can disable it by setting ``tcp_keep_alive`` to False and you can control keepalive parameters
by new ``tcp_keep_alive_*`` parameters added to constructor of the Hook, Operator and Sensor. Setting the
TCP_KEEPALIVE prevents some firewalls from closing a long-running connection that has long periods of
inactivity by sending empty TCP packets periodically. This has a very small impact on network traffic,
and potentially prevents the idle/hanging connections from being closed automatically by the firewalls.

* ``Add TCP_KEEPALIVE option to http provider (#24967)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``fix document about response_check in HttpSensor (#24708)``
   * ``Fix HttpHook.run_with_advanced_retry document error (#24380)``
   * ``Remove 'xcom_push' flag from providers (#24823)``
   * ``Move provider dependencies to inside provider folders (#24672)``
   * ``Remove 'hook-class-names' from provider.yaml (#24702)``

3.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

.. note::
  This release of provider is only available for Airflow 2.2+ as explained in the
  `Apache Airflow providers support policy <https://github.com/apache/airflow/blob/main/PROVIDERS.rst#minimum-supported-version-of-airflow-for-community-managed-providers>`_.

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Migrate HTTP example DAGs to new design AIP-47 (#23991)``
   * ``Add explanatory note for contributors about updating Changelog (#24229)``
   * ``Prepare docs for May 2022 provider's release (#24231)``
   * ``Update package description to remove double min-airflow specification (#24292)``

2.1.2
.....

Bug Fixes
~~~~~~~~~

* ``Fix mistakenly added install_requires for all providers (#22382)``

2.1.1
.....

Misc
~~~~~

* ``Add Trove classifiers in PyPI (Framework :: Apache Airflow :: Provider)``

2.1.0
.....

Features
~~~~~~~~

* ``Add 'method' to attributes in HttpSensor. (#21831)``

Misc
~~~~

* ``Support for Python 3.10``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Add pre-commit check for docstring param types (#21398)``

2.0.3
.....

Misc
~~~~

* ``Split out confusing path combination logic to separate method (#21247)``


.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
   * ``Add documentation for January 2021 providers release (#21257)``

2.0.2
.....

Bug Fixes
~~~~~~~~~

   * ``Un-ignore DeprecationWarning (#20322)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Fix MyPy Errors for HTTP provider. (#20246)``
   * ``Update documentation for November 2021 provider's release (#19882)``
   * ``Prepare documentation for October Provider's release (#19321)``
   * ``Update documentation for September providers release (#18613)``
   * ``Static start_date and default arg cleanup for misc. provider example DAGs (#18597)``
   * ``Use typed Context EVERYWHERE (#20565)``
   * ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
   * ``Even more typing in operators (template_fields/ext) (#20608)``
   * ``Update documentation for provider December 2021 release (#20523)``

2.0.1
.....

Misc
~~~~

* ``Optimise connection importing for Airflow 2.2.0``
* ``Remove airflow dependency from http provider``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Update description about the new ''connection-types'' provider meta-data (#17767)``
   * ``Import Hooks lazily individually in providers manager (#17682)``
   * ``Prepares docs for Rc2 release of July providers (#17116)``
   * ``Remove/refactor default_args pattern for miscellaneous providers (#16872)``
   * ``Prepare documentation for July release of providers. (#17015)``
   * ``Removes pylint from our toolchain (#16682)``

2.0.0
.....

Breaking changes
~~~~~~~~~~~~~~~~

* ``Auto-apply apply_default decorator (#15667)``

.. warning:: Due to apply_default decorator removal, this version of the provider requires Airflow 2.1.0+.
   If your Airflow version is < 2.1.0, and you want to install this provider version, first upgrade
   Airflow to at least version 2.1.0. Otherwise your Airflow package version will be upgraded
   automatically and you will have to manually run ``airflow upgrade db`` to complete the migration.

Features
~~~~~~~~

* ``Update 'SimpleHttpOperator' to take auth object (#15605)``
* ``HttpHook: Use request factory and respect defaults (#14701)``

.. Below changes are excluded from the changelog. Move them to
   appropriate section above if needed. Do not delete the lines(!):
   * ``Check synctatic correctness for code-snippets (#16005)``
   * ``Prepares provider release after PIP 21 compatibility (#15576)``
   * ``Remove Backport Providers (#14886)``
   * ``Updated documentation for June 2021 provider release (#16294)``
   * ``Add documentation for the HTTP connection (#15379)``
   * ``More documentation update for June providers release (#16405)``
   * ``Synchronizes updated changelog after buggfix release (#16464)``

1.1.1
.....

Bug fixes
~~~~~~~~~

* ``Corrections in docs and tools after releasing provider RCs (#14082)``


1.1.0
.....

Updated documentation and readme files.

Features
~~~~~~~~

* ``Add a new argument for HttpSensor to accept a list of http status code``

1.0.0
.....

Initial version of the provider.
