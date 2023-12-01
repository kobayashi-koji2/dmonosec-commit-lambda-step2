# CodeCommit運用方針

以下の「技術開発本部のCodeCommit運用フロー」に沿って運用する

## 構成について

### 基本構造

[main]  readme、ソースプッシュ禁止  
│  
[master] ☆リリースバージョンを管理する  
│  
├─ [master-dev] ☆全機能評価を行う  
││  
│└─ [dev] ☆複数人で開発したコードを共有する   
││  
│└─ [u999999-***] PR作成用ソースブランチ（社員番号6桁で明示）  
│  
├─ [master-sta] ☆本番リリースをリハーサルする   
│  
└─ [master-pro] ★本番リリースを行う  

## ソースコードのマージ

### ☆について

- ソースコードをマージする際は、プルリクエストを作成する
- プルリクエストは、「STS の担当者1名」の承認後、早送りマージする

### ★について

- ソースコードをマージする際は、プルリクエストを作成する
- プルリクエストは、「STS の責任者1名」の承認後、早送りマージする

# Lambda
## Lambda layer
```
dmonosec-commit-terraform-step2/
    └ module/
        └ Lambda/
            └ layer_file/
                ┝ common_functions.zip
                │    ├ db.py       # DynamoDB操作
                │    ├ ssm.py      # パラメータストア値取得
                │    └ convert.py  # 型変換
                │
                └ jose.zip # idtokeデコードモジュール
```