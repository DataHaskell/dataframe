{
  description = "A fast, safe, and intuitive DataFrame library.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pinch = {
      url = "github:abhinav/pinch";
      flake = false;
    };
    network-run = {
      url = "github:kazu-yamamoto/network-run";
      flake = false;
    };
    granite = {
      url = "github:mchav/granite";
      flake = false;
    };
    haskell-ci-flake = {
      url = ./nix/haskell-ci-flake;
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        hsPkgs = pkgs.haskellPackages.extend (
          self: super: {
            haskell-ci = inputs.haskell-ci-flake.packages.${system}.default;
            network-run = self.callCabal2nix "network-run" inputs.network-run { };
            dataframe-arrow = self.callCabal2nix "dataframe-arrow" ./dataframe-arrow { };
            dataframe-core = self.callCabal2nix "dataframe-core" ./dataframe-core { };
            dataframe-csv = self.callCabal2nix "dataframe-csv" ./dataframe-csv { };
            dataframe-csv-th = self.callCabal2nix "dataframe-csv-th" ./dataframe-csv-th { };
            dataframe-expr-serializer =
              self.callCabal2nix "dataframe-expr-serializer" ./dataframe-expr-serializer
                { };
            dataframe-fastcsv = self.callCabal2nix "dataframe-fastcsv" ./dataframe-fastcsv { };
            # dataframe-fusion = self.callCabal2nix "dataframe-fusion" ./dataframe-fusion { };
            dataframe-hasktorch = self.callCabal2nix "dataframe-hasktorch" ./dataframe-hasktorch { };
            dataframe-huggingface = self.callCabal2nix "dataframe-huggingface" ./dataframe-huggingface { };
            dataframe-json = self.callCabal2nix "dataframe-json" ./dataframe-json { };
            dataframe-lazy = self.callCabal2nix "dataframe-lazy" ./dataframe-lazy { };
            dataframe-learn = self.callCabal2nix "dataframe-learn" ./dataframe-learn {
              parallel = pkgs.haskell.lib.dontCheck (self.callHackage "parallel" "3.3.0.0" { });
            };
            dataframe-operations = self.callCabal2nix "dataframe-operations" ./dataframe-operations { };
            dataframe-parquet = self.callCabal2nix "dataframe-parquet" ./dataframe-parquet {
              pinch = self.callCabal2nix "pinch" inputs.pinch { };
            };
            dataframe-parquet-th = self.callCabal2nix "dataframe-parquet-th" ./dataframe-parquet-th { };
            dataframe-parsing = self.callCabal2nix "dataframe-parsing" ./dataframe-parsing { };
            dataframe-persistent = self.callCabal2nix "dataframe-persistent" ./dataframe-persistent { };
            dataframe-th = self.callCabal2nix "dataframe-th" ./dataframe-th { };
            dataframe-viz = self.callCabal2nix "dataframe-viz" ./dataframe-viz {
              granite = self.callCabal2nix "granite" inputs.granite { };
            };
            dataframe = self.callCabal2nix "dataframe" ./. { };
          }
        );
      in
      {
        packages = {
          default = hsPkgs.dataframe;
          dataframe = hsPkgs.dataframe;
          dataframe-arrow = hsPkgs.dataframe-arrow;
          dataframe-core = hsPkgs.dataframe-core;
          dataframe-csv = hsPkgs.dataframe-csv;
          dataframe-csv-th = hsPkgs.dataframe-csv-th;
          dataframe-fastcsv = hsPkgs.dataframe-fastcsv;
          # dataframe-fusion = hsPkgs.dataframe-fusion;
          dataframe-hasktorch = hsPkgs.dataframe-hasktorch;
          dataframe-json = hsPkgs.dataframe-json;
          dataframe-lazy = hsPkgs.dataframe-lazy;
          dataframe-learn = hsPkgs.dataframe-learn;
          dataframe-operations = hsPkgs.dataframe-operations;
          dataframe-parquet = hsPkgs.dataframe-parquet;
          dataframe-parquet-th = hsPkgs.dataframe-parquet-th;
          dataframe-parsing = hsPkgs.dataframe-parsing;
          dataframe-persistent = hsPkgs.dataframe-persistent;
          dataframe-th = hsPkgs.dataframe-th;
          dataframe-viz = hsPkgs.dataframe-viz;
        };

        devShells.default = hsPkgs.shellFor {
          packages = ps: [
            ps.dataframe
            ps.dataframe-arrow
            ps.dataframe-core
            ps.dataframe-csv
            ps.dataframe-csv-th
            ps.dataframe-expr-serializer
            ps.dataframe-fastcsv
            # ps.dataframe-fusion
            ps.dataframe-hasktorch
            ps.dataframe-huggingface
            ps.dataframe-json
            ps.dataframe-lazy
            ps.dataframe-learn
            ps.dataframe-operations
            ps.dataframe-parquet
            ps.dataframe-parquet-th
            ps.dataframe-parsing
            ps.dataframe-persistent
            ps.dataframe-th
            ps.dataframe-viz
          ];
          nativeBuildInputs = with hsPkgs; [
            ghc
            cabal-install
            haskell-language-server
            haskell-ci
          ];
          withHoogle = true;
        };
      }
    );
}
