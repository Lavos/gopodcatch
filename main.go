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
	"time"
)

const (
	MAX_DOWNLOADS = 3
)

var (
	date_formats = []string{
		time.RFC1123,
		time.RFC1123Z,
		time.RFC3339,
	}
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
	PubDate string `xml:"pubDate"`
}

type Enclosure struct {
	URL string `xml:"url,attr"`
}


// communication types
type FeedUpdate struct {
	Id int64
	Items []Item
}

type Download struct {
	Id  int64
	URL string
}

func setupDatabase () {
	conn, _ := sqlite3.Open("podcast.db")
	defer conn.Close()

	conn.Exec("DROP TABLE IF EXISTS feeds")
	conn.Exec("DROP TABLE IF EXISTS items")
	conn.Exec("CREATE TABLE feeds (id INTEGER PRIMARY KEY ASC, name TEXT, url TEXT)")
	conn.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY ASC, feed INTEGER, url TEXT UNIQUE ON CONFLICT IGNORE, pubdate INTEGER, downloaded INTEGER DEFAULT 0)")

	conn.Exec("CREATE INDEX IF NOT EXISTS urls ON items (url)")

	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Retronauts', 'http://retronauts.libsyn.com/rss')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('In-Game Chat', 'http://www.ingamechat.net/?feed=podcast')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Giant Bombcast', 'http://www.giantbomb.com/podcast-xml/giant-bombcast/')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Weekend Confirmed', 'http://www.shacknews.com/extras/podcast/weekendconfirmed.xml')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('The Game Informer Show', 'http://feeds.feedburner.com/gameinformershow')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Gamers with Jobs', 'http://www.gamerswithjobs.com/taxonomy/term/408/0/feed')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('8-4 Play', 'http://eightfour.libsyn.com/rss')")
	// conn.Exec("INSERT INTO feeds (name, url) VALUES ('PC Gamer Podcast', 'http://www.pcgamer.com/feed/')")

	conn.Commit()
}

func updateItems () {
	feed_count := 0
	items := make(map[int64][]Item)
	success := make(chan FeedUpdate)
	failure := make(chan bool)

	conn, _ := sqlite3.Open("podcast.db")
	defer conn.Close()

	row := make(sqlite3.RowMap)

	for s, err := conn.Query("SELECT id, url FROM feeds"); err == nil; err = s.Next() {
		s.Scan(row)

		feed_count++
		go updateFeed(row["id"].(int64), row["url"].(string), success, failure)
	}

	log.Printf("feeds found: %v", feed_count)

	for index := 0; index < feed_count; index++ {
		select {
		case update := <-success:
			log.Printf("got new items: %v", update)
			items[update.Id] = update.Items
		case <-failure:
		}
	}

	log.Printf("items found: %v", len(items))

	for id, itemlist := range items {
		for _, item := range itemlist {
			args := sqlite3.NamedArgs{
				"$id": id,
				"$url": item.Enclosure.URL,
				"$pubdate": parseDate(item.PubDate).Unix(),
			}

			i_err := conn.Exec("INSERT INTO items (feed, url, pubdate) VALUES ($id, $url, $pubdate)", args)
			log.Printf("i_err: %v", i_err)
		}
	}

	conn.Commit()
}

func parseDate (date_string string) time.Time {
	for _, f := range date_formats {
		pubdate, err := time.Parse(f, date_string)

		if err == nil {
			return pubdate;
		}
	}

	return time.Now()
}

func updateFeed (feed int64, url string, success chan FeedUpdate, failure chan bool) {
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

	success <- FeedUpdate{ feed, rss.Channel.ItemList }
}

func downloadItems () {
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

	download_limit := len(downloads)
	log.Printf("items to download: %v", download_limit)

	if download_limit == 0 {
		log.Print("No new items found.")
		return
	}

	var init_max int

	if download_limit < MAX_DOWNLOADS {
		init_max = download_limit
	} else {
		init_max = MAX_DOWNLOADS
	}

	for i := 0; i < init_max; i++ {
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

func pop (list []Download) (Download, []Download) {
	return list[len(list)-1], list[:len(list)-1]
}

func downloadEnclosure (d Download, success chan int64, failure chan bool) {
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

func main () {
	// setupDatabase()
	updateItems()
	downloadItems()
}
