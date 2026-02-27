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

        hsPkgs = pkgs.haskellPackages.extend (self: super: {
          dataframe = self.callCabal2nix "dataframe" ./. { };
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
