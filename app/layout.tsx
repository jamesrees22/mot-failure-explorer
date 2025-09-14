import "./globals.css";
import type { ReactNode } from "react";

export const metadata = {
  title: "MOT Failure Explorer",
  description: "UK MOT failure insights (2024)"
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <head>
        <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;700&display=swap" rel="stylesheet" />
      </head>
      <body className="bg-gray-900 text-white font-roboto">
        <header className="sticky top-0 z-40 border-b border-red-800 bg-gray-800/70 backdrop-blur-md">
          <div className="mx-auto max-w-7xl px-5 py-4 flex items-center justify-between">
            <div className="text-accent text-2xl font-bold text-red-600">MOT / OPS CONSOLE</div>
            <div className="text-sm text-gray-400">v1.0 • LIVE</div>
          </div>
        </header>
        <main className="mx-auto max-w-7xl px-6 py-8">{children}</main>
      </body>
    </html>
  );
}