/**
 * @license
 * SPDX-License-Identifier: Apache-2.0
 */

export default function App() {
  return (
    <div className="w-full h-screen overflow-hidden bg-[#0D1B2A]">
      <iframe 
        src="/dashboard.html" 
        className="w-full h-full border-none"
        title="yellow.ai Bot Health Monitor"
      />
    </div>
  );
}
