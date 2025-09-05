export const metadata = { title: "MOT Failure Explorer" };
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "system-ui, sans-serif", margin: 0, padding: 24, background: "#0b1220", color: "#e5e7eb" }}>
        {children}
      </body>
    </html>
  );
}
