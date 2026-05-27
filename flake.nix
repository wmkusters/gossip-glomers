{
  description = "A very basic flake";

  inputs = {
    #nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-25.11";
    # pin nixpkgs to maelstrom 0.2.3
    nixpkgs.url = "github:NixOS/nixpkgs/59133ee770406f605d61698bc4f1a89efcf461d5";
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-pinned,
    }:
    let
      systems = [
        "aarch64-darwin"
        "x86_64-linux"
      ];
      forEachSystem =
        f:
        builtins.listToAttrs (
          map (system: {
            name = system;
            value = f nixpkgs.legacyPackages.${system};
          }) systems
        );
    in
    {
      devShells = forEachSystem (pkgs: {
        default = pkgs.mkShell {
          packages = [ pkgs.maelstrom ];
        };
      });
    };
}
