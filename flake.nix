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
          hash = "sha256-Z/o8gxMOBltKiaL0NEjMUyOvUljRvKErWeM6Ul3GM9k=";
        };

        hsPkgs = pkgs.haskellPackages.extend (self: super: {
          granite = self.callCabal2nix "granite" granitePkg { };
          dataframe-fastcsv = self.callCabal2nix "dataframe-fastcsv" ./dataframe-fastcsv { };
          dataframe-persistent = self.callCabal2nix "dataframe-persistent" ./dataframe-persistent { };
          dataframe-hasktorch = self.callCabal2nix "dataframe-hasktorch" ./dataframe-hasktorch { };
          dataframe = self.callCabal2nix "dataframe" ./. { };
        });
      in
      {
        packages = {
          default = hsPkgs.dataframe;
          dataframe = hsPkgs.dataframe;
          dataframe-fastcsv = hsPkgs.dataframe-fastcsv;
          dataframe-hasktorch = hsPkgs.dataframe-hasktorch;
          dataframe-persistent = hsPkgs.dataframe-persistent;
        };

        devShells.default = hsPkgs.shellFor {
          packages = ps: [
            ps.dataframe
            ps.dataframe-fastcsv
            ps.dataframe-persistent
            ps.dataframe-hasktorch
          ];
          nativeBuildInputs = with hsPkgs; [
            ghc
            cabal-install
            haskell-language-server
          ];
          withHoogle = true;
        };
      });
}
