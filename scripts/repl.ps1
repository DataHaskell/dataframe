chcp 65001
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:GHC_CHARENC = "UTF-8"

cabal repl dataframe --repl-options=-fobject-code -O2 --build-depends=text --build-depends=vector --build-depends=bytestring --build-depends=time
