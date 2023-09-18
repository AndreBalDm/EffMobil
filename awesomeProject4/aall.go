package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
	"log"
	"net/http"
	"strings"
)

type FIO struct {
	Name       string
	Surname    string
	Patrynomic string
}
type FIOFull struct {
	Name       string
	Surname    string
	Patrynomic string
	Age        int
	Gender     string
	National   string
}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

func InsertData(conn *pgx.Conn, order FIOFull) (err error) { //записываем данные в БД
	query := `
		insert into fiofull
			(name, surname, patronymic, age, gender, national) 
		values 
			($1,$2,$3,$4,$5,$6)
		`
	if _, err = conn.Exec(context.Background(), query,
		order.Name,
		order.Surname,
		order.Patrynomic,
		order.Age,
		order.Gender,
		order.National,
	); err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			return pgErr
		}
	}
	return nil
}
func fillbyurl[T any, Z getter[T]](url, parametr string, result *T) error {
	answer, err := preddictbyurl[T, Z](url, parametr)
	if err != nil {
		return err
	}
	*result = answer
	return nil
}

type ageffiresp struct {
	Age int `json:"age"`
}

func (o ageffiresp) Get() int {
	return o.Age
}

type genderfiresp struct {
	Gender string `json:"gender"`
}

func (o genderfiresp) Get() string {
	return o.Gender
}

type Countrydetails struct {
	Countryid   string  `json:"country_id"`
	Probability float64 `json:"probability"`
}

type nationalfiresp struct {
	Country []Countrydetails `json:"country"`
}

func (o nationalfiresp) Get() string {
	p := 0.00
	r := ""
	for _, c := range o.Country {
		if c.Probability > p {
			p = c.Probability
			r = c.Countryid
		}
	}
	return r
}

type getter[T any] interface {
	Get() T
}

func preddictbyurl[T any, Z getter[T]](url string, parametr string) (T, error) {
	// Отправка GET-запроса к API
	response, err := http.Get(url + "?name=" + parametr)
	if err != nil {
		fmt.Println("Ошибка при отправке запроса:", err)
		return *new(T), err
	}
	defer response.Body.Close()
	resp := new(Z)
	// Чтение данных из ответа
	err = json.NewDecoder(response.Body).Decode(resp)
	if err != nil {
		fmt.Println("Ошибка при чтении данных:", err)
		return *new(T), err
	}
	return (*resp).Get(), nil
}

func predictfromFIO(val FIO) (FIOFull, error) {
	url1, url2, url3 := "https://api.agify.io/", "https://api.genderize.io/", "https://api.nationalize.io/"
	var age int
	var gender, national string
	result := FIOFull{}
	g := errgroup.Group{}
	g.Go(func() error {
		return fillbyurl[int, ageffiresp](url1, val.Name, &age)
	})
	g.Go(func() error {
		return fillbyurl[string, genderfiresp](url2, val.Name, &gender)
	})
	g.Go(func() error {
		return fillbyurl[string, nationalfiresp](url3, val.Name, &national)
	})
	//g.Go(func() error {
	//	var err error
	//	age, err = preddictbyurl(url1, val.Name)
	//	return err
	//})
	//g.Go(func() error {
	//	var err error
	//	gender, err = preddictbyurl(url2, val.Name)
	//	return err
	//})
	//g.Go(func() error {
	//	var err error
	//	national, err = preddictbyurl(url3, val.Name)
	//	return err
	//})
	err := g.Wait()
	if err != nil {
		return result, err
	}
	result.Name = val.Name
	result.Surname = val.Surname
	result.Patrynomic = val.Patrynomic
	result.Age = age
	result.Gender = gender
	result.National = national
	return result, nil
}

func main() {
	// get kafka reader using environment variables.
	kafkaURL := "localhost:9092" //os.Getenv("kafkaURL")
	topic := "my-topic-1"        //os.Getenv("topic")
	groupID := "andre"           //os.Getenv("groupID")
	reader := getKafkaReader(kafkaURL, topic, groupID)

	db, err := createdb("postgres://postgres:2508@localhost:5432/postgres1")
	if err != nil {
		fmt.Println("no connect")
		return
	}
	defer db.Close(context.Background())
	/*
		go startkafkaconsumer(reader, db)
	*/
	startkafkaconsumer(reader, db)
	//	startapi(db)

	// Создаем клиент Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Адрес Redis сервера
		Password: "",               // Пароль Redis сервера (если требуется)
		DB:       0,                // Индекс базы данных Redis
	})
	// Выполняем запрос к PostgreSQL для получения данных
	rows, _ := db.Query(context.Background(), "SELECT id, name FROM fiofull")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	// Итерируемся по результатам запроса и сохраняем данные в Redis
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}
		// Сохраняем данные в Redis
		err = redisClient.Set(context.Background(), fmt.Sprintf("FIOFull:%d", id), name, 0).Err()
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Данные успешно сохранены в Redis.")
}

func createdb(conn string) (*pgx.Conn, error) {
	var Sqlconfig = `
	create table if not exists FIOFull
	(
		id      serial primary key,
		name    varchar(30),
		surname   varchar(20),
		patronymic     varchar(20),
		age    int,
		gender varchar(25),
		national  varchar(20)
		);`

	db, err := pgx.Connect(context.Background(), conn)
	if err != nil {
		return nil, err
	}
	err = db.Ping(context.Background())
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(context.Background(), Sqlconfig)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func startkafkaconsumer(reader *kafka.Reader, conn *pgx.Conn) {
	defer reader.Close()
	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		val := FIO{}
		err = json.Unmarshal(m.Value, &val)
		if err != nil {
			fmt.Println("FIO not valid")
			continue
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %v\n", m.Topic, m.Partition, m.Offset, string(m.Key), val)

		dbentty, err := predictfromFIO(val)
		if err != nil {
			fmt.Println("no pred")
			continue
		}

		err = InsertData(conn, dbentty)
		if err != nil {
			fmt.Println("no inasert")
			continue
		}
	}
}
