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
        <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet" />
      </head>
      <body className="bg-gray-100 font-roboto text-gray-800">
        <div className="flex h-screen">
          <aside className="w-64 bg-white shadow-lg p-4">
            <h2 className="text-xl font-semibold text-gray-700 mb-4">MOT Explorer</h2>
            <nav>
              <ul className="space-y-2">
                <li><a href="#" className="text-blue-600 hover:text-blue-800">Overview</a></li>
                <li><a href="#" className="text-gray-600 hover:text-gray-800">Failures</a></li>
                <li><a href="#" className="text-gray-600 hover:text-gray-800">Trends</a></li>
              </ul>
            </nav>
          </aside>
          <div className="flex-1 flex flex-col">
            <header className="bg-white shadow-md p-4 flex items-center justify-between">
              <h1 className="text-2xl font-bold text-gray-800">MOT Failure Explorer</h1>
              <div className="text-sm text-gray-500">Last Update: 2024-12-31 23:59 UTC</div>
            </header>
            <main className="p-6 overflow-auto">{children}</main>
          </div>
        </div>
      </body>
    </html>
  );
}