package types

type DailyExport struct {
	Id           int64    `parquet:"name=id, type=INT64" json:"id"`
	Parent       *int64   `parquet:"name=parent, type=INT64" json:"parent"`
	Thread       int64    `parquet:"name=thread, type=INT64" json:"thread"`
	Forum        string   `parquet:"name=forum, type=BYTE_ARRAY" json:"forum"`
	Message      string   `parquet:"name=message, type=BYTE_ARRAY" json:"message"`
	CreatedAt    string   `parquet:"name=createdAt, type=BYTE_ARRAY" json:"createdAt"`
	IsHatespeech *bool    `parquet:"name=isHatespeech, type=BOOLEAN" json:"isHatespeech"`
	HsScore      *float64 `parquet:"name=hs_score, type=DOUBLE" json:"hs_score"`
}
