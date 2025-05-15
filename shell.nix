{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    (pkgs.python311.withPackages (ps: [
      ps.numpy
      ps.pandas
      ps.pip
      ps.setuptools
    ]))
  ];

  shellHook = ''
    # Configure pip to install packages locally
    export PIP_PREFIX=$(pwd)/_pip_packages
    export PYTHONPATH="$PIP_PREFIX/lib/python3.11/site-packages:$PYTHONPATH"
    export PATH="$PIP_PREFIX/bin:$PATH"
    
    # Install yfinance if not already installed
    if ! pip show yfinance &>/dev/null; then
      pip install -q yfinance
    fi
  '';
}
