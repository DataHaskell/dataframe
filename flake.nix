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
          dataframe = self.callCabal2nix "dataframe" ./. {
            inherit granite;
          };
        });
      in
      {
        packages = {
          default = hsPkgs.dataframe;
        };

        devShells.default = hsPkgs.shellFor {
          packages = ps: [ (ps.callCabal2nix "dataframe" ./. { }) ];
          nativeBuildInputs = with pkgs; [
            ghc
            cabal-install
            haskell-language-server
          ];
          withHoogle = true;
        };
      });
}
