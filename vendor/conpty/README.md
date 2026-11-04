# ConPTY 1.24.260710001

`conpty.dll` and `OpenConsole.exe` in `x64/` and `arm64/` are Microsoft's ConPTY from the NuGet
package
[`Microsoft.Windows.Console.ConPTY` 1.24.260710001](https://www.nuget.org/packages/Microsoft.Windows.Console.ConPTY/1.24.260710001)
(source: [microsoft/terminal](https://github.com/microsoft/terminal)), unmodified, under the MIT
license in `LICENSE`. All four files carry Microsoft's Authenticode signature.

| File | Package path | SHA-256 |
|---|---|---|
| package `microsoft.windows.console.conpty.1.24.260710001.nupkg` | | `175640566a3b59c4b132070ee96c2c77e5ab7edd2e92732a5eb3610bbf63d90e` |
| `x64/conpty.dll` | `runtimes/win-x64/native/conpty.dll` | `39fba2713e2495117b1591ae8c32a3b904bea7aa66069cf7815e2844c76d75d8` |
| `x64/OpenConsole.exe` | `build/native/runtimes/x64/OpenConsole.exe` | `b7fd936c2668b87b9ecf7b3366dc6568afc1c6f981874cba3e955a1c35cf8160` |
| `arm64/conpty.dll` | `runtimes/win-arm64/native/conpty.dll` | `db3d173640b172bafd42d5b541b638a9aeec1c7d0e40dd636bf02822a32c912c` |
| `arm64/OpenConsole.exe` | `build/native/runtimes/arm64/OpenConsole.exe` | `ed7622fd0d3bedc9ab9f122f5e58edf0def9e7999224f52dd395ba9f54edbe09` |

`src/conpty.rs` embeds the files of the target architecture in Windows x64 and arm64 builds and
loads this ConPTY in place of the system one. To update, replace all four files from a newer
package, update the hashes here and `VERSION` in `src/conpty.rs`.
