import React from 'react';
import './App.css';
var apigClientFactory = require('./apigClient').default;

class ChatBox extends React.Component {

  constructor(){
    super();
    this.state = {
      messages : [],
      messageToSend: "",
      apigClient : null
    }

    this.sendMessage = this.sendMessage.bind(this);
    this.changeMessage = this.changeMessage.bind(this);

    this.drone = new window.Scaledrone("ntBW8mNeY82G4H9A", {
      data: this.state.member
    });

    this.drone.on('open', error => {
      if (error) {
        return console.error(error);
      }
      const member = {...this.state.member};
      member.id = this.drone.clientId;
      this.setState({member});
    });
    const room = this.drone.subscribe("observable-room");
    room.on('data', (data, member) => {
      const messages = this.state.messages;
      messages.push({member, text: data});
      this.setState({messages});
    });
  }

  changeMessage(event) {
    this.setState({
      messageToSend : event.target.value
    })
  }

  sendMessage() {
    let newMessage = this.state.messageToSend;
    let message = {
      text: newMessage,
      sender: 'user'
    }
    this.setState({
      messages : this.state.messages.concat(message)
    });

    this.setState({
      messageToSend: ""
    });

    
    let UnstructuredMessage = {
      "id" : "0",
      "text" : newMessage,
      "timestamp" : Date(Date.now()).toString()
    }
    
    let Message = {
        "type" : "string",
        "unstructured" : UnstructuredMessage
    }
    
    let botRequest = {
        "messages": [Message]
    }

    this.state.apigClient.chatbotPost(null, botRequest)
    .then((response) => {
      let responseMessage = JSON.parse(response.data.body).messages[0].unstructured.text;
      let botMessage = {
        text: responseMessage,
        sender: 'bot'
      }
      this.setState({
        messages : this.state.messages.concat(botMessage)
      });
    })
    .catch((result) => {
      console.error(result);
    });

  }

  componentDidMount() {
    var apigClient = apigClientFactory.newClient({
      apiKey: 'VhW2bJJpT91y9Ac502dyb3BP7nJqwRf68Z5oFd1e'
    });

    this.setState({
      apigClient : apigClient
    });
  }

  onSubmit(e) {
    e.preventDefault();
    // this.setState({text: ""});
    // this.props.onSendMessage(this.state.text);
  }

  render() {

    return (
      <div className="App">
      {/* <div lassName="blurred-box"></div> */}
      <div className="App-header">
          <h1>Dining Concierge</h1>
        </div>
          {/* <div className="chatBody"> */}
            <ul className="Messages-list">
                {this.state.messages.map((message, index) =>
                  // <ChatBubble message={message} />
                  <li className={"Messages-message " + message.sender}>
                      <div className="Message-content" key={index} >
                        <div className="text">
                          {message.text}
                        </div>
                  </div>
                  </li>
                  )}
              </ul>
          {/* </div> */}
          <div className="Input">
          <form onSubmit={e => this.onSubmit(e)}>
          <input
            onChange={this.changeMessage}
            value={this.state.messageToSend}
            type="text"
            placeholder="Enter your message and press ENTER"
            autoFocus="{true}"
          />
          <button onClick={this.sendMessage} > Send </button>
          </form>
      </div>
      </div>
    )
  }
}

function App() {
  return (
        <div className="col m-4">
          <ChatBox />
        </div>
  );
}

export default App;
