set(MGR_DASHBOARD_V2_VIRTUALENV ${CEPH_BUILD_VIRTUALENV}/mgr-dashboard_v2-virtualenv)

add_custom_target(mgr-dashboard_v2
  COMMAND
  ${CMAKE_SOURCE_DIR}/src/tools/setup-virtualenv.sh ${MGR_DASHBOARD_V2_VIRTUALENV} &&
  ${MGR_DASHBOARD_V2_VIRTUALENV}/bin/pip install --no-index --use-wheel --find-links=file:${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2/wheelhouse -r requirements.txt
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/mgr/dashboard_v2
  COMMENT "mgr-dashboard_v2 is being created")
add_dependencies(tests mgr-dashboard_v2)

include(Distutils)
distutils_install_module(mgr_dashboard_v2)
