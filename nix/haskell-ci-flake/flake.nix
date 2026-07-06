{
  description = "Local haskell-ci flake to avoid cluttering main flake.nix";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    haskell-ci.url = "github:haskell-CI/haskell-ci";
    haskell-ci.flake = false;
  };

  outputs =
    {
      nixpkgs,
      flake-utils,
      haskell-ci,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        hsPkgs = pkgs.haskellPackages.extend (
          self: super: {
            haskell-ci = pkgs.haskell.lib.dontCheck (
              self.callCabal2nixWithOptions "haskell-ci" haskell-ci "--flag -ShellCheck" {
                cabal-install-parsers = pkgs.haskell.lib.dontCheck (
                  self.callCabal2nix "cabal-install-parsers" "${haskell-ci}/cabal-install-parsers" {
                    Cabal-syntax = pkgs.haskell.lib.dontCheck (self.callHackage "Cabal-syntax" "3.14.2.0" { });
                  }
                );
                Cabal-syntax = pkgs.haskell.lib.dontCheck (self.callHackage "Cabal-syntax" "3.14.2.0" { });
                optparse-applicative = self.callHackage "optparse-applicative" "0.19.0.0" { };
              }
            );
          }
        );
      in
      {
        packages = {
          default = hsPkgs.haskell-ci;
        };
      }
    );
}
