import React, { Component, PropTypes } from 'react';
import { Panel, Alert, Button,
        ButtonToolbar, Form } from 'react-bootstrap';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './Alert.css';

const title = 'Alert';

class AlertsPage extends Component {

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  constructor(props) {
    super(props);
    this.state = {
      alertVisible1: true,
      alertVisible2: true,
      alertVisible3: true,
      alertVisible4: false,
      alertValue: '',
      addNewAlert: false,
      showGrowlDefault: false,
      showGrowlPrimary: false,
      showGrowlInfo: false,
      showGrowlWarning: false,
      showGrowlError: false,
    };
    this.handleAlertDismiss1 = this.handleAlertDismiss1.bind(this);
    this.handleAlertShow1 = this.handleAlertShow1.bind(this);
    this.handleAlertDismiss2 = this.handleAlertDismiss2.bind(this);
    this.handleAlertShow2 = this.handleAlertShow2.bind(this);
    this.handleAlertDismiss3 = this.handleAlertDismiss3.bind(this);
    this.handleAlertShow3 = this.handleAlertShow3.bind(this);
    this.handleAlertDismiss4 = this.handleAlertDismiss4.bind(this);
    this.handleAlertShow4 = this.handleAlertShow4.bind(this);
    this.addAlert = this.addAlert.bind(this);
    this.toggleGrowl = this.toggleGrowl.bind(this);
    this.closeGrowl = this.closeGrowl.bind(this);
  }

  componentWillMount() {
    this.context.setTitle(title);
  }

  handleAlertDismiss1() {
    this.setState({ alertVisible1: false });
  }

  handleAlertShow1() {
    this.setState({ alertVisible1: true });
  }
  handleAlertDismiss2() {
    this.setState({ alertVisible2: false });
  }

  handleAlertShow2() {
    this.setState({ alertVisible2: true });
  }
  handleAlertDismiss3() {
    this.setState({ alertVisible3: false });
  }

  handleAlertShow3() {
    this.setState({ alertVisible3: true });
  }
  handleAlertDismiss4() {
    this.setState({ alertVisible4: false });
  }

  handleAlertShow4() {
    this.setState({ alertVisible4: true });
  }

  addAlert(e) {
    e.preventDefault();
    this.setState({
      alertVisible4: true,
      addNewAlert: true,
    });
  }

  toggleGrowl(e) {
    const that = this;
    switch (e.target.name) {
      case 'Default':
        this.setState({ showGrowlDefault: !this.state.showGrowlDefault });
        setTimeout(() => {
          that.setState({ showGrowlDefault: false });
        }, 5000);
        break;
      case 'Primary':
        this.setState({ showGrowlPrimary: !this.state.showGrowlPrimary });
        setTimeout(() => {
          that.setState({ showGrowlPrimary: false });
        }, 5000);
        break;
      case 'Info':
        this.setState({ showGrowlInfo: !this.state.showGrowlInfo });
        setTimeout(() => {
          that.setState({ showGrowlInfo: false });
        }, 5000);
        break;
      case 'Warning':
        this.setState({ showGrowlWarning: !this.state.showGrowlWarning });
        setTimeout(() => {
          that.setState({ showGrowlWarning: false });
        }, 5000);
        break;
      case 'Error':
        this.setState({ showGrowlError: !this.state.showGrowlError });
        setTimeout(() => {
          that.setState({ showGrowlError: false });
        }, 5000);
        break;
      default : this.setState({ showGrowlDefault: !this.state.showGrowlDefault });
        setTimeout(() => {
          that.setState({ showGrowlDefault: false });
        }, 5000);
    }
  }

  closeGrowl(e) {
    switch (e.target.getAttribute('name')) {
      case 'Default':
        this.setState({ showGrowlDefault: false });
        break;
      case 'Primary':
        this.setState({ showGrowlPrimary: false });
        break;
      case 'Info':
        this.setState({ showGrowlInfo: false });
        break;
      case 'Warning':
        this.setState({ showGrowlWarning: false });
        break;
      case 'Error':
        this.setState({ showGrowlError: false });
        break;
      default: this.setState({ showGrowlDefault: false });
    }
  }

  render() {
    return (
      <div className="animate">
        <Panel
          header={<span>Dismissable Alerts</span>}
          bsStyle="primary"
        >
          {this.state.alertVisible1 ?
            <Alert bsStyle="danger" onDismiss={this.handleAlertDismiss1}>
              <p>Oh snap! Change a few things up and try submitting again.</p>
              <p>
                <Button onClick={this.handleAlertDismiss1}>Hide Alert</Button>
              </p>
            </Alert>
          : <Button onClick={this.handleAlertShow1} className={s.showAlert}>Show Alert</Button>}

          {this.state.alertVisible2 ?
            <Alert bsStyle="info" onDismiss={this.handleAlertDismiss2}>
              <p>Ok! Not bad, but you can do better.</p>
              <p>
                <Button onClick={this.handleAlertDismiss2}>Hide Alert</Button>
              </p>
            </Alert>
          : <Button onClick={this.handleAlertShow2} className={s.showAlert}>Show Alert</Button>}

          {this.state.alertVisible3 ?
            <Alert bsStyle="success" onDismiss={this.handleAlertDismiss3}>
              <p>Well done! You successfully read this important alert message.</p>
              <p>
                <Button onClick={this.handleAlertDismiss3}>Hide Alert</Button>
              </p>
            </Alert>
          : <Button onClick={this.handleAlertShow3} className={s.showAlert}>Show Alert</Button>}

          {this.state.alertVisible4 ?
            <Alert
              bsStyle="warning"
              onDismiss={this.handleAlertDismiss4}
              style={{ display: this.state.addNewAlert ? '' : 'none' }}
            >
              <p>Oh snap! Change a few things up and try submitting again.</p>
              <p>
                <Button onClick={this.handleAlertDismiss4} >Hide Alert</Button>
              </p>
            </Alert>
          : <Button
            onClick={this.handleAlertShow4}
            style={{ display: this.state.addNewAlert ? '' : 'none' }}
            className={s.showAlert}
          >
            Show Alert
          </Button>}

          <Form inline onSubmit={this.addAlert}>
            <Button type="submit"> Add Alert </Button>
          </Form>
        </Panel>

        <Panel
          header={<span>Growl Alerts</span>}
          bsStyle="info"
        >
          <ButtonToolbar>
            <Button bsStyle="default" onClick={this.toggleGrowl} name="Default">
              Show Growl
            </Button>
            <Button bsStyle="primary" onClick={this.toggleGrowl} name="Primary">
              Primary Growl
            </Button>
            <Button bsStyle="info" onClick={this.toggleGrowl} name="Info">
              Info Growl
            </Button>
            <Button bsStyle="warning" onClick={this.toggleGrowl} name="Warning">
              Warning Growl
            </Button>
            <Button bsStyle="danger" onClick={this.toggleGrowl} name="Error">
              Danger Growl
            </Button>
          </ButtonToolbar>

          <growl-notifications>
            <growl-notification
              class="fading"
              style={{ display: this.state.showGrowlDefault ? '' : 'none' }}
            >
              <a
                href=""
                onClick={this.closeGrowl} className="close"
              >
                <i className="fa fa-times" name="Default" /></a>
              Default Noti
            </growl-notification>

            <growl-notification
              class="growl-primary fading"
              style={{ display: this.state.showGrowlPrimary ? '' : 'none' }}
            >
              <a
                href=""
                onClick={this.closeGrowl} className="close"
              >
                <i className="fa fa-times" name="Primary" /></a>
              Primary Noti
            </growl-notification>

            <growl-notification
              class="growl-info fading"
              style={{ display: this.state.showGrowlInfo ? '' : 'none' }}
            >
              <a
                href=""
                onClick={this.closeGrowl} className="close"
              >
                <i className="fa fa-times" name="Info" /></a>
              Info Noti
            </growl-notification>

            <growl-notification
              class="growl-warning"
              style={{ display: this.state.showGrowlWarning ? '' : 'none' }}
            >
              <a
                href=""
                onClick={this.closeGrowl} className="close"
              >
                <i className="fa fa-times" name="Warning" /></a>
              Warning Notification
            </growl-notification>

            <growl-notification
              class="growl-error fading"
              style={{ display: this.state.showGrowlError ? '' : 'none' }}
            >
              <a
                href=""
                onClick={this.closeGrowl} className="close"
              >
                <i className="fa fa-times" name="Error" /></a>
              Error Noti
            </growl-notification>
          </growl-notifications>

        </Panel>

      </div>
    );
  }
}

export default withStyles(s)(AlertsPage);
