package store

import (
	"os"
	"path/filepath"
	"strconv"
)

func DataToHint(path string) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	finfo, err := f.Stat()
	if err != nil {
		return
	}
	if finfo.IsDir() {
		return DataToHintDir(path, 0, MAX_CHUNK_ID)
	} else {

		return DataToHintFile(path)
	}
	return
}

func DataToHintDir(path string, start, end int) (err error) {
	bkt := &Bucket{}
	conf.HintConfig.SplitCap = 10 << 20
	bkt.datas = NewdataStore(0, path)
	bkt.hints = newHintMgr(0, path)
	_, err = bkt.datas.ListFiles()
	if err != nil {
		return
	}

	for i := start; i <= end; i++ {
		data := bkt.datas.genPath(i)
		_, err := os.Stat(data)
		if err != nil {
			continue
		}
		logger.Infof("building %s", data)
		err = bkt.checkHintWithData(i)
		if err != nil {
			logger.Errorf("error build %s", i)
		}
	}
	return
}

func DataToHintFile(path string) (err error) {
	dir := filepath.Dir(path)
	name := filepath.Base(path)
	chunkID, err := strconv.Atoi(name[:3])
	if err != nil {
		return
	}
	DataToHintDir(dir, chunkID, chunkID)

	return
}
