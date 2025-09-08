import "./globals.css";
import type { ReactNode } from "react";

export const metadata = {
  title: "MOT Failure Explorer",
  description: "UK MOT failure insights (2024)"
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header className="sticky top-0 z-40 border-b border-white/5 bg-bg/70 backdrop-blur">
          <div className="mx-auto max-w-7xl px-5 py-3 flex items-center justify-between">
            <div className="text-accent font-mono text-lg">MOT / OPS CONSOLE</div>
            <div className="text-xs text-soft font-mono">v1.0 • LIVE</div>
          </div>
        </header>
        <main className="mx-auto max-w-7xl px-5 py-6">{children}</main>
      </body>
    </html>
  );
}
