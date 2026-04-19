{
  description = "A fast, safe, and intuitive DataFrame library.";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        granitePkg = pkgs.fetchFromGitHub {
          repo = "granite";
          owner = "mchav";
          rev = "main";
          hash = "sha256-ypDiV99w2J8q7XcMpFWkv0kEm2dtWZduWIkAsuXDHEo=";
        };

        hsPkgs = pkgs.haskellPackages.extend (self: super: rec {
          granite = self.callCabal2nix "granite" granitePkg { };
          dataframe-fastcsv = self.callCabal2nix "dataframe-fastcsv" ./dataframe-fastcsv {
            inherit parallel;
          };
          dataframe = self.callCabal2nix "dataframe" ./. {
            inherit granite;
          };
          parallel = super.parallel_3_3_0_0;
        });
      in
      {
        packages = {
          default = hsPkgs.dataframe;
          dataframe = hsPkgs.dataframe;
          dataframe-fastcsv = hsPkgs.dataframe-fastcsv;
        };

        devShells.default = hsPkgs.shellFor {
          packages = ps: [ ps.dataframe ps.dataframe-fastcsv ];
          nativeBuildInputs = with hsPkgs; [
            ghc
            cabal-install
            haskell-language-server
          ];
          withHoogle = true;
        };
      });
}
