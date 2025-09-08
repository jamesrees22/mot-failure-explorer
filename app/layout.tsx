import "./globals.css";
import type { ReactNode } from "react";

export const metadata = {
  title: "MOT Failure Explorer 2024",
  description: "Cyberpunk-styled dashboard for UK MOT failure insights (2024)."
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <div className="min-h-screen grid grid-cols-[260px_1fr]">
          <aside className="bg-panel border-r border-white/5 p-5">
            <div className="text-accent font-mono text-xl mb-6">TACTICAL OPS</div>
            <nav className="space-y-2">
              <a className="block px-3 py-2 rounded-lg bg-bg/40 hover:bg-bg/70 transition font-mono text-soft" href="/">Command Center</a>
              <a className="block px-3 py-2 rounded-lg hover:bg-bg/40 transition font-mono text-soft" href="#">Agent Network</a>
              <a className="block px-3 py-2 rounded-lg hover:bg-bg/40 transition font-mono text-soft" href="#">Operations</a>
              <a className="block px-3 py-2 rounded-lg hover:bg-bg/40 transition font-mono text-soft" href="#">Intelligence</a>
            </nav>
            <div className="mt-8 text-xs text-soft font-mono">v2.1.7 • CLASSIFIED</div>
          </aside>
          <main className="p-6">{children}</main>
        </div>
      </body>
    </html>
  );
}
