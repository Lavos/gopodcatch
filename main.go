package main

import (
	"code.google.com/p/go-sqlite/go1/sqlite3"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	MAX_DOWNLOADS = 3
)

// RSS xml parsing types
type RSS struct {
	Channel Channel `xml:"channel"`
}

type Channel struct {
	ItemList []Item `xml:"item"`
}

type Item struct {
	Enclosure Enclosure `xml:"enclosure"`
}

type Enclosure struct {
	URL string `xml:"url,attr"`
}

type Download struct {
	Id  int64
	URL string
}

func setupDatabase() {
	conn, _ := sqlite3.Open("podcast.db")
	defer conn.Close()

	conn.Exec("DROP TABLE IF EXISTS feeds")
	conn.Exec("DROP TABLE IF EXISTS items")
	conn.Exec("CREATE TABLE feeds (id INTEGER PRIMARY KEY ASC, name TEXT, url TEXT)")
	conn.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY ASC, url TEXT UNIQUE ON CONFLICT IGNORE, downloaded INTEGER DEFAULT 0)")

	conn.Exec("CREATE INDEX IF NOT EXISTS urls ON items (url)")

	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Retronauts', 'http://retronauts.libsyn.com/rss')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('In-Game Chat', 'http://www.ingamechat.net/?feed=podcast')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Giant Bombcast', 'http://www.giantbomb.com/podcast-xml/giant-bombcast/')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Weekend Confirmed', 'http://www.shacknews.com/extras/podcast/weekendconfirmed.xml')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('The Game Informer Show', 'http://feeds.feedburner.com/gameinformershow')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Gamers with Jobs', 'http://www.gamerswithjobs.com/taxonomy/term/408/0/feed')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('8-4 Play', 'http://eightfour.libsyn.com/rss')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('PC Gamer Podcast', 'http://www.pcgamer.com/feed/')")


	conn.Commit()
}

func updateItems() {
	feed_count := 0
	items := make([]Item, 0)
	success := make(chan []Item)
	failure := make(chan bool)

	conn, _ := sqlite3.Open("podcast.db")
	defer conn.Close()

	row := make(sqlite3.RowMap)

	for s, err := conn.Query("SELECT url FROM feeds"); err == nil; err = s.Next() {
		s.Scan(row)

		feed_count++
		go updateFeed(row["url"].(string), success, failure)
	}

	log.Printf("feeds found: %v", feed_count)

	for index := 0; index < feed_count; index++ {
		select {
		case new_items := <-success:
			log.Printf("got new items: %v", new_items)
			items = append(items, new_items...)
		case <-failure:
		}
	}

	log.Printf("items found: %v", len(items))

	for _, item := range items {
		log.Printf("insert item: %v", item)
		i_err := conn.Exec("INSERT INTO items (url) VALUES (?)", item.Enclosure.URL)
		log.Printf("i_err: %v", i_err)
	}

	conn.Commit()
}

func updateFeed(url string, success chan []Item, failure chan bool) {
	resp, err := http.Get(url)
	defer resp.Body.Close()

	if err != nil {
		failure <- true
		return
	}

	decoder := xml.NewDecoder(resp.Body)

	var rss RSS
	d_err := decoder.Decode(&rss)

	if d_err != nil {
		log.Printf("[decoder error] %v", d_err)
		failure <- true
		return
	}

	success <- rss.Channel.ItemList
}

func downloadItems() {
	success := make(chan int64)
	failure := make(chan bool)
	downloads := make([]Download, 0)
	success_ids := make([]int64, 0)

	conn, _ := sqlite3.Open("podcast.db")
	defer conn.Close()

	row := make(sqlite3.RowMap)

	for i, err := conn.Query("SELECT url, id FROM items WHERE downloaded = 0"); err == nil; err = i.Next() {
		i.Scan(row)
		downloads = append(downloads, Download{Id: row["id"].(int64), URL: row["url"].(string)})
	}

	log.Printf("urls: %v", downloads)
	download_limit := len(downloads)

	if download_limit == 0 {
		log.Print("No new items found.")
		return
	}

	for i := 0; i < MAX_DOWNLOADS; i++ {
		var d Download
		d, downloads = pop(downloads)
		go downloadEnclosure(d, success, failure)
	}

	for n := 0; n < download_limit; n++ {
		select {
		case id := <-success:
			success_ids = append(success_ids, id)
		case <-failure:
		}

		if len(downloads) != 0 {
			var d Download
			d, downloads = pop(downloads)
			go downloadEnclosure(d, success, failure)
		}

		log.Print("got result, looping...")
	}

	log.Printf("successful ids: %v", success_ids)
	id_list := make([]string, 0)
	for _, id := range success_ids {
		id_list = append(id_list, strconv.FormatInt(id, 10))
	}

	stmt := fmt.Sprintf("UPDATE items SET downloaded = 1 WHERE id IN (%v)", strings.Join(id_list, ","))
	id_err := conn.Exec(stmt)

	log.Printf("id_err: %v", id_err)
	conn.Commit()
}

func pop(list []Download) (Download, []Download) {
	return list[len(list)-1], list[:len(list)-1]
}

func downloadEnclosure(d Download, success chan int64, failure chan bool) {
	resp, err := http.Get(d.URL)
	defer resp.Body.Close()

	log.Printf("downloading: %v", d.URL)

	if err != nil {
		failure <- true
		return
	}

	_, filename := path.Split(d.URL)
	out, _ := os.Create(fmt.Sprintf("./dl/%s", filename))
	defer out.Close()

	_, dl_err := io.Copy(out, resp.Body)

	if dl_err != nil {
		failure <- true
		return
	}

	success <- d.Id
}

func main() {
	// setupDatabase()
	updateItems()
	// downloadItems()
}
