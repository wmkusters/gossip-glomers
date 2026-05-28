{
  description = "Flake for gossip glomers";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    # pin to maelstrom 0.2.3
    maelstromPinPkgs.url = "github:NixOS/nixpkgs/59133ee770406f605d61698bc4f1a89efcf461d5";
  };

  outputs =
    {
      self,
      nixpkgs,
      maelstromPinPkgs,
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
            value = f {
              pkgs = nixpkgs.legacyPackages.${system};
              maelstromPkgs = maelstromPinPkgs.legacyPackages.${system};
            };
          }) systems
        );
      services = {
        echo = {
          subPackage = "echo";
          bin = "echo";
          maelstromArgs = "-w echo --node-count 1 --time-limit 10";
        };
        unique-id-generation = {
          subPackage = "unique-id-generation";
          bin = "unique-id-generation";
          maelstromArgs = "-w unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition";
        };
        broadcast = {
          subPackage = "broadcast";
          bin = "broadcast";
          maelstromArgs = "-w broadcast --time-limit 20 --rate 100 --node-count 25 --latency 100";
        };
      };
    in
    {
      packages = forEachSystem (
        { pkgs, ...}:
        builtins.mapAttrs (
          name: cfg:
          pkgs.buildGoModule {
            inherit name;
            src = ./.;
            subPackages = [ cfg.subPackage ];
            vendorHash = "sha256-b0Vt2rIgPRjYorCc8qmKPoQL0L3DOGurfftAr58jQHA=";
          }
        ) services
      );
      apps = forEachSystem (
        { pkgs, maelstromPkgs }:
        let
          system = pkgs.system;
          sharedRuntimeDeps = [ maelstromPkgs.maelstrom-clj ];
        in
        builtins.mapAttrs (name: cfg: {
          type = "app";
          program = toString (
            pkgs.writeShellScript name ''
              export PATH="${pkgs.lib.makeBinPath sharedRuntimeDeps}:$PATH"
              exec maelstrom test \
                --bin ${self.packages.${system}.${name}}/bin/${cfg.bin} \
                ${cfg.maelstromArgs} \
                "$@"
            ''
          );
        }) services
      );
    };
}
