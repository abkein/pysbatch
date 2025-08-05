{ lib, python3 }:

python3.pkgs.buildPythonPackage {
  pname = "pysbatch";
  version = "0.0.4";
  format = "pyproject";

  src = ./.;

  # install hatchling *into* the build environment so that
  # PEP 517 sees hatchling.build as an available backend:
  nativeBuildInputs = with python3.pkgs; [
    hatchling
  ];

  propagatedBuildInputs = with python3.pkgs; [
    marshmallow
    paramiko
    toml
  ];

  pythonImportsCheck = [ "pysbatch" ];

  # nativeCheckInputs = with python3.pkgs; [ pytest ];
  # checkPhase = ''
  #   pytest
  # '';

  meta = with lib; {
    description = "Python library for managing SLURM batch jobs";
    homepage = "";
    license = licenses.mit;
    maintainers = with maintainers; [ ];
    platforms = platforms.unix;
    broken = false;
  };
}