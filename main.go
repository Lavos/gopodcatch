package main

import (
	"log"
	"code.google.com/p/go-sqlite/go1/sqlite3"
	"encoding/xml"
	"net/http"
	"io"
	"os"
	"path"
	"strings"
	"strconv"
)

const (
	MAX_DOWNLOADS = 5
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


func setupDatabase () {
	conn, _ := sqlite3.Open("podcast.db")
	defer conn.Close()

	conn.Exec("DROP TABLE IF EXISTS feeds")
	conn.Exec("DROP TABLE IF EXISTS items")
	conn.Exec("CREATE TABLE feeds (id INTEGER PRIMARY KEY ASC, name TEXT, url TEXT)")
	conn.Exec("CREATE TABLE items (id INTEGER PRIMARY KEY ASC, url TEXT UNIQUE ON CONFLICT IGNORE, downloaded INTEGER DEFAULT 0)")

	conn.Exec("INSERT INTO feeds (name, url) VALUES ('Retronauts', 'http://retronauts.libsyn.com/rss')")
	conn.Exec("INSERT INTO feeds (name, url) VALUES ('In-Game Chat', 'http://www.ingamechat.net/?feed=podcast')")
	conn.Commit()
}


func getNewDownloads () {
	var feed_count int

	items := make([]Item, 0)
	feed_responses := make(chan []Item)

	/* */
	var download_count int
	dl_success := make(chan int64)
	dl_failure := make(chan bool)
	successes := make([]int64, 0)
	/* */

	conn, _ := sqlite3.Open("podcast.db")
	defer conn.Close()

	row := make(sqlite3.RowMap)

	for s, err := conn.Query("SELECT url FROM feeds"); err == nil; err = s.Next() {
		s.Scan(row)

		feed_count++
		go updateFeed(row["url"].(string), feed_responses)
	}

	log.Printf("feeds found: %v", feed_count)

	for index := 0; index < feed_count; index++ {
		new_items := <-feed_responses
		log.Printf("got new items: %v", new_items)
		items = append(items, new_items...)
	}

	log.Printf("items found: %v", len(items))

	/* /
	for _, item := range items {
		conn.Exec("INSERT OR IGNORE INTO items (url) VALUES (?)", item.Enclosure.URL)
	}
	/* */

	conn.Commit()

	/* */
	for i, err := conn.Query("SELECT url, id FROM items WHERE downloaded = 0"); err == nil; err = i.Next() {
		i.Scan(row)
		download_count++

		go downloadEnclosure(row["url"].(string), row["id"].(int64), dl_success, dl_failure)
	}

	log.Printf("downloads found: %v", download_count)

	for d_index := 0; d_index < download_count; d_index++ {
		select {
			case int := <-dl_success:
				successes = append(successes, int)
			case <-dl_failure:
		}
	}

	log.Printf("successful ids: %v", successes)
	id_list := make([]string, 0)
	for _, id := range successes {
		id_list = append(id_list, strconv.FormatInt(id, 10))
	}

	log.Printf("id list: %v", id_list)
	log.Printf("id list joined: %v", strings.Join(id_list, ","))

	id_err := conn.Exec("UPDATE items SET downloaded = 1 WHERE id IN (?)", strings.Join(id_list, ","))

	log.Printf("id_err: %v", id_err)
	conn.Commit()

	/* */
}

func updateFeed (url string, returnchan chan []Item) {
	resp, err := http.Get(url)
	defer resp.Body.Close()

	if err != nil {
		// handle error
	}

	decoder := xml.NewDecoder(resp.Body)

	var rss RSS
	d_err := decoder.Decode(&rss)

	if d_err != nil {
		log.Printf("[decoder error] %v", d_err)
	}

	returnchan <- rss.Channel.ItemList
}

func downloadEnclosure (url string, id int64, successchan chan int64, failurechan chan bool) {
	resp, err := http.Get(url)
	defer resp.Body.Close()

	log.Printf("downloading: %v", url)

	if err != nil {
		failurechan <- true
		return
	}

	_, filename := path.Split(url)
	out, _ := os.Create(filename)
	defer out.Close()

	_, dl_err := io.Copy(out, resp.Body)

	if dl_err != nil {
		failurechan <- true
		return
	}

	successchan <- id
}

func main () {
	// setupDatabase()
	getNewDownloads()
}
