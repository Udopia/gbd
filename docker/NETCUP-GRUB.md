# Netcup GRUB: prevention & rescue cheatsheet (private, not committed)

Notes for the GBD VPS (`v2202212190561213394`, Netcup KVM, Debian). GRUB hung once
after the buster→bullseye reboot and had to be rescued via the Netcup SCP rescue
system. This is how to prevent a repeat and how to recover if it happens again.

## Disk layout (GPT + BIOS boot)

```
sda1  2M    → BIOS boot partition (bios_grub): holds GRUB core.img
sda2  1G    → /boot
sda3  1023G → /
```

`grub-install /dev/sda` embeds GRUB into the 2 MB `sda1`. Root is `sda3`, `/boot`
is the separate `sda2` (both must be mounted for a chroot rescue).

## Why it hung

New GRUB package updates the modules under `/boot/grub`, but the bootloader in
`sda1` isn't re-embedded unless `grub-pc`'s install device is set: and GRUB's
graphical terminal (`gfxterm`) can hang on the virtual display. Fix both.

## Prevention: run BEFORE any reboot / dist-upgrade (as root)

1. Snapshot the VPS in the Netcup SCP first (instant rollback).
2. In `/etc/default/grub` set:
   ```
   GRUB_TERMINAL=console
   GRUB_TIMEOUT=5
   GRUB_TIMEOUT_STYLE=menu
   GRUB_CMDLINE_LINUX_DEFAULT="console=tty0 console=ttyS0,115200"
   ```
   Comment out any `GRUB_GFXMODE`, `GRUB_GFXPAYLOAD_LINUX`, `GRUB_HIDDEN_TIMEOUT*`.
3. Pin the install device, re-embed GRUB, regenerate config:
   ```bash
   echo "grub-pc grub-pc/install_devices multiselect /dev/sda" | debconf-set-selections
   grub-install /dev/sda
   update-grub
   ```
4. Verify a kernel + config exist before rebooting:
   ```bash
   ls -l /boot/vmlinuz-* /boot/initrd.img-*
   grep -E "menuentry|^\s*linux|^\s*initrd" /boot/grub/grub.cfg | head
   ```
5. Reboot to validate the boot path in isolation (snapshot = safety net).

During a dist-upgrade, when `grub-pc` prompts for the install device, tick
`/dev/sda`; keep the hardened `/etc/default/grub`. Re-run `grub-install /dev/sda
&& update-grub` before the post-upgrade reboot.

## Rescue (if it hangs again)

Netcup SCP → boot the **rescue system**, then:

```bash
lsblk                                   # confirm sda3 = /, sda2 = /boot
mount /dev/sda3 /mnt
mount /dev/sda2 /mnt/boot
for d in dev proc sys run; do mount --rbind /$d /mnt/$d; done
chroot /mnt

# fix bootloader (and edit /etc/default/grub as above if needed)
grub-install /dev/sda
update-grub

exit
```

Then switch boot back to the normal disk in the SCP and reboot.

## Context

- The GBD stack (nginx + gbd) runs in Docker; all data is in bind mounts under
  `/home/iser` and `/etc/letsencrypt`, untouched by an OS upgrade or GRUB fix.
- certbot is a snap; it survives OS upgrades.
- Debian 11 (bullseye) LTS ends ~end of Aug 2026 → plan bookworm (12) before long,
  calmly, with the prevention steps above.
