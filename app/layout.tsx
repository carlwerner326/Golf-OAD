import "./globals.css";
import type { ReactNode } from "react";

export const metadata = {
  title: "Golf One & Done Pool",
  description: "Season-long One & Done pool manager"
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body>
        <main>{children}</main>
      </body>
    </html>
  );
}
