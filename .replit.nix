{ pkgs }: {
  deps = [
    pkgs.nodejs-18_x
    pkgs.nodePackages.npm
    pkgs.nodePackages.typescript-language-server

    # Playwright runtime OS deps
    pkgs.chromium
    pkgs.glib
    pkgs.nspr
    pkgs.nss
    pkgs.dbus
    pkgs.at-spi2-core
    pkgs.cups
    pkgs.gtk3
    pkgs.pango
    pkgs.cairo
    pkgs.libdrm
    pkgs.mesa
    pkgs.alsaLib
    pkgs.libxkbcommon
    pkgs.xorg.libX11
    pkgs.xorg.libXcomposite
    pkgs.xorg.libXdamage
    pkgs.xorg.libXext
    pkgs.xorg.libXfixes
    pkgs.xorg.libXrandr
    pkgs.xorg.libxcb
  ];
}

