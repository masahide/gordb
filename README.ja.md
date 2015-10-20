# gordb


## streamに指定できる関数の種類

* selection  行の選択
* projection 列の選択
* rename 列名の変更
* union 行の結合
* join 列の結合
* crossjoin すべて列の組み合わせ
* relation  表そのもの(メモリに読み込まれたcsv)
* iselection  行の選択(full-index版)

## 関数

### selection

<pre><code class="json">
"selection": {
	"input":       対象ストリーム,
	"attr":           比較カラム名,
	"selector":  比較演算子(">",">=","<","<=","==","!=") ,
	"arg":            比較する値 (数値/文字列)
}
</code></pre>

### projection

<pre><code class="json">
"projection": {
	"input":       対象ストリーム,
	"attrs":  [ 対象カラム, 対象カラム,... ]
}
</code></pre>

### rename

<pre><code class="json">
"rename": {
	"input":       対象ストリーム,
	"from":         対象カラム名,
	"to":              変更後カラム名
}
</code></pre>

### union

<pre><code class="json">
"union": {
	"input1":       対象ストリーム1,
	"input1":       対象ストリーム2
}
</code></pre>

### join

<pre><code class="json">
"join": {
	"input1":       対象ストリーム1,
	"input1":       対象ストリーム2,
	"attr1":          対象ストリーム1の結合カラム名,
	"attr2":          対象ストリーム2の結合カラム名,
	"selector":  比較演算子(">",">=","<","<=","==","!=") 
}
</code></pre>

### crossjoin

<pre><code class="json">
"crossjoin": {
	"input1":       対象ストリーム1,
	"input1":       対象ストリーム2
}
</code></pre>

### relation

<pre><code class="json">
"relation":{
	"name":       csv名(メモリ上のパス名)
}
</code></pre>

### iselection

<pre><code class="json">
"iselection": {
	"input":   { csv名(メモリ上のパス名)  },
	"attr":           比較カラム名,
	"selector":  比較演算子 (">",">=","<","<=","==") ,
	"arg":            比較する値 (数値/文字列)
}
</code></pre>

