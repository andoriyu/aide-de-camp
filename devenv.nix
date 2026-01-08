{ pkgs, lib, config, inputs, ... }: {
  packages = [
    pkgs.bacon
    pkgs.cargo-cache
    pkgs.cargo-deny
    pkgs.cargo-diet
    pkgs.cargo-llvm-cov
    pkgs.cargo-nextest
    pkgs.cargo-outdated
    pkgs.cargo-sort
    pkgs.cargo-sweep
    pkgs.cargo-wipe
    pkgs.cargo-release
    pkgs.git-cliff
    pkgs.cmake
    pkgs.gnumake
    pkgs.pkg-config
    pkgs.openssl.dev
    pkgs.sqlite
    pkgs.sqlx-cli
    pkgs.just
  ];

  languages.rust = {
    enable = true;
    channel = "stable";
    components = ["rustc" "cargo" "clippy" "rustfmt" "rust-src" "llvm-tools-preview"];
  };

  services.postgres = {
    enable = true;
    listen_addresses = ""; # Unix socket only
    initialDatabases = [
      { name = "aide_de_camp_dev"; }
    ];
    initialScript = ''
      CREATE USER aide_de_camp WITH PASSWORD 'aide_de_camp';
      GRANT ALL PRIVILEGES ON DATABASE aide_de_camp_dev TO aide_de_camp;
      \c aide_de_camp_dev
      GRANT ALL ON SCHEMA public TO aide_de_camp;
    '';
  };

  git-hooks.hooks = {
    rustfmt.enable = true;
    clippy = {
      enable = true;
      settings.denyWarnings = true;
    };
    cargo-check.enable = true;
  };

  enterShell = ''
    echo "aide-de-camp development environment"
    export DATABASE_URL="postgresql:///aide_de_camp_dev?host=$PGHOST"
  '';
}
