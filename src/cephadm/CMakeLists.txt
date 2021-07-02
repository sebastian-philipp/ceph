if(WITH_TESTS)
  include(AddCephTest)
  add_tox_test(cephadm TOX_ENVS py3 mypy flake8)
endif()

set(bin_target_file ${CMAKE_BINARY_DIR}/bin/cephadm)

add_custom_command(
    OUTPUT "${bin_target_file}"
    DEPENDS "${CMAKE_SOURCE_DIR}/src/cephadm/cephadm.py"
    COMMAND bash build.sh "${bin_target_file}"
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/cephadm
    COMMENT "compiling cephadm"
)

add_custom_target(cephadm-cephadm
  ALL
  DEPENDS ${bin_target_file}
)
