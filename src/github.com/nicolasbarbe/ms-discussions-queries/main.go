package main

import (
  "github.com/julienschmidt/httprouter"
  "github.com/unrolled/render"
  "github.com/nicolasbarbe/kafka"
  "gopkg.in/mgo.v2"
  "net/http"
  "strings"
  "log"
  "os"
)

/** Constants **/

const (
  discussionsCollection = "discussions"
  discussionsTopic      = "discussions"
  discussionStarted     = "DiscussionStarted"

  usersCollection       = "users"
  usersTopic            = "users"
  userCreated           = "UserCreated"
)

var (
    brokers               = strings.Split(os.Getenv("KAFKA_BROKERS"), ",")  
    usersQueriesCS        = os.Getenv("USERS_QUERIES_CS")
)

/** Main **/

func main() {

  // create kafka producer
  var producer = kafka.NewProducer(brokers)

  // create http renderer
  var renderer = render.New(render.Options{
      IndentJSON: true,
  })

  // create mongodb session
  session, err := mgo.Dial(os.Getenv("MONGODB_CS"))
  if err != nil {
    log.Fatal("Cannot connect to mongo server: %s", err)
  }

  mongo := session.DB(os.Getenv("MONGODB_DB"))

  // create controller
  controller := Controller {
    mongo    : mongo,
    producer : producer,
    renderer : renderer,
  }

  log.Println("Initializing consumers...")

  // listen to the creation of new users
  // usersConsumer := kafka.NewConsumer(brokers, usersTopic)
  // usersConsumer.Consume(controller.ConsumeUsers )

    // listen to the creation of new discussions
  discussionsConsumer := kafka.NewConsumer(brokers, discussionsTopic)
  discussionsConsumer.Consume(controller.ConsumeDiscussions )

  log.Println("Initializing routes...")

  // create routes
  router := httprouter.New()
  router.GET("/api/v1/discussions", controller.ListDiscussions)
  router.GET("/api/v1/discussions/:id", controller.ShowDiscussion)
  
  log.Println("Start listening...")

  log.Fatal(http.ListenAndServe(":8080", router))

  defer func() {
    producer.Close()
    session.Close()
    // usersConsumer.Close()
    discussionsConsumer.Close()
  }()
}