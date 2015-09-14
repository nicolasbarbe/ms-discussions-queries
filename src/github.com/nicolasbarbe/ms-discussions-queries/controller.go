package main

import ( 
        "github.com/julienschmidt/httprouter"
        "github.com/unrolled/render"
        "gopkg.in/mgo.v2"
        "gopkg.in/mgo.v2/bson"
        "github.com/nicolasbarbe/kafka"
        "encoding/json" 
        "net/http"
        "strconv"
        "io/ioutil"
        "time"
        "math/rand"
        "fmt"
        "log"
)


/** Types **/

// Discussion representation used in the event body
type NormalizedDiscussion struct {
  Id            string            `json:"id"            bson:"_id"`
  Title         string            `json:"title"         bson:"title"`
  Description   string            `json:"description"   bson:"description"`
  Initiator     string            `json:"initiator"     bson:"initiator"`
  CreatedAt     time.Time         `json:"createdAt"     bson:"createdAt"`
}

type DenormalizedDiscussion struct {
  Id            string            `json:"id"            bson:"_id"`
  Title         string            `json:"title"         bson:"title"`
  Description   string            `json:"description"   bson:"description"`
  Initiator     string            `json:"initiator"     bson:"initiator"`
  CreatedAt     time.Time         `json:"createdAt"     bson:"createdAt"`
  Answers       []string          `json:"answers"       bson:"answers"`
  Up            int               `json:"up"            bson:"up"`
  Views         int               `json:"views"         bson:"views"`
}

type DenormalizedUser struct {
  Id            string            `json:"id"            bson:"_id"`
  FirstName     string            `json:"firstName"     bson:"firstName"`
  LastName      string            `json:"lastName"      bson:"lastName"`
  MemberSince   time.Time         `json:"memberSince"   bson:"memberSince"`
}

type NormalizedAnswer struct {
  Id            string             `json:"id"            bson:"_id"`
  Content       string             `json:"content"       bson:"content"`
  Author        string             `json:"author"        bson:"author"`
  CreatedAt     time.Time          `json:"createdAt"     bson:"createdAt"`
  Discussion    string             `json:"discussion"    bson:"discussion"`
}

// Controller embeds the logic of the microservice
type Controller struct {
  mongo         *mgo.Database
  producer      *kafka.Producer
  renderer      *render.Render
}

func (this *Controller) ListDiscussions(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {
  var discussions []DenormalizedDiscussion
  if err := this.mongo.C(discussionsCollection).Find(nil).Limit(100).All(&discussions) ; err != nil {
    log.Print(err)
  }
  this.renderer.JSON(response, http.StatusOK, discussions)
}

func (this *Controller) ShowDiscussion(response http.ResponseWriter, request *http.Request, params httprouter.Params ) {
  var discussion DenormalizedDiscussion
  if err := this.mongo.C(discussionsCollection).FindId(params.ByName("id")).One(&discussion) ; err != nil {
    this.renderer.JSON(response, http.StatusNotFound, "Discussion not found")
    return
  }
   
  this.renderer.JSON(response, http.StatusOK, discussion)
}

func (this *Controller) ConsumeDiscussions(message []byte) {
  idx, _    := strconv.Atoi(string(message[:2]))
  eventType := string(message[2:idx+2])
  body      := message[idx+2:]

  if eventType != discussionStarted {
    log.Printf("Message with type %v is ignored. Type %v was expected", eventType, discussionStarted)
    return
  }

  // unmarshal discussion from event body
  var normalizedDiscussion NormalizedDiscussion
  if err := json.Unmarshal(body, &normalizedDiscussion); err != nil {
    log.Print("Cannot unmarshal discussion")
    return
  }

  // fetch user details
  var user DenormalizedUser 
  if err := this.fetchResource(usersQueriesCS, normalizedDiscussion.Initiator, &user) ; err != nil {
    log.Print("Cannot fetch resource: ", err)
    return
  }

  // create internal representation
  denormalizedDiscussion := DenormalizedDiscussion {
   Id            : normalizedDiscussion.Id,            
   Title         : normalizedDiscussion.Title,      
   Description   : normalizedDiscussion.Description,
   Initiator     : fmt.Sprintf("%v %v", user.FirstName, user.LastName), 
   CreatedAt     : normalizedDiscussion.CreatedAt, 
   Up            : rand.Intn(100),
   Views         : rand.Intn(3000),
  }

  // save discussion
  if err := this.mongo.C(discussionsCollection).Insert(denormalizedDiscussion) ; err != nil {
    log.Printf("Cannot save document in collection %s : %s", discussionsCollection, err)
    return
  }
}

func (this *Controller) ConsumeAnswers(message []byte) {
  idx, _    := strconv.Atoi(string(message[:2]))
  eventType := string(message[2:idx+2])
  body      := message[idx+2:]

  if eventType != answerPosted {
    log.Printf("Message with type %v is ignored. Type %v was expected", eventType, answerPosted)
    return
  }

  // unmarshal answer from event body
  var normalizedAnswer NormalizedAnswer
  if err := json.Unmarshal(body, &normalizedAnswer); err != nil {
    log.Print("Cannot unmarshal answer")
    return
  }

  if err := this.mongo.C(discussionsCollection).Update(bson.M{"_id": normalizedAnswer.Discussion}, bson.M{"$addToSet": bson.M{"answers": normalizedAnswer.Id}}) ; err != nil {
    log.Printf("Cannot update document in collection %s : %s", discussionsCollection, err)
    return
  }
}


func (this *Controller) fetchResource(cs string, id string, resource interface{}) error {
  resp, err := http.Get("http://" + cs + "/api/v1/users/" + id)

  if err != nil {
    return fmt.Errorf("Cannot retrieve resource with identifier %v from service %v", id, cs)
  }

  defer resp.Body.Close()

  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
      return fmt.Errorf("Cannot read resource with identifier %v from service %v", id, cs)
  }
  
  // unmarshall resource
  if err := json.Unmarshal(body, resource); err != nil {
    return fmt.Errorf("Cannot unmarshal resources with identifier %v", id)
  }
  return nil
}


