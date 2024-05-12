import React, { Component, PropTypes } from 'react';
import { Panel, ButtonToolbar, Button, ButtonGroup } from 'react-bootstrap';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './Button.css';

const title = 'Buttons';

class Buttons extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }
  componentDidMount() {
    [].slice.call(document.querySelectorAll('button.progress-button')).forEach((bttn) => {
      new ProgressButton(bttn, { //eslint-disable-line
        callback: (instance) => {
          let progress = 0;
          const interval = setInterval(() => {
            progress = Math.min(progress + (Math.random() * 0.1), 1);
            instance._setProgress(progress); //eslint-disable-line

            if (progress === 1) {
              instance._stop(1); // eslint-disable-line
              clearInterval(interval);
            }
          }, 200);
        },
      });
    });
  }
  render() {
    return (
      <div className="animate">
        <div className="row">
          <div className="col-sm-12">
            <Panel
              header={<span>Buttons</span>}
              bsStyle="primary"
            >
              <ButtonToolbar>
                <Button bsStyle="default" className="btn-rounded">Default Button</Button>
                <Button bsStyle="primary" className="btn-rounded">Primary Button</Button>
                <Button bsStyle="success" className="btn-rounded">Success Button</Button>
                <Button bsStyle="info" className="btn-rounded">Info Button</Button>
                <Button bsStyle="warning" className="btn-rounded">Warning Button</Button>
                <Button bsStyle="danger" className="btn-rounded">Danger Button</Button>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <Button bsStyle="default">Default Button</Button>
                <Button bsStyle="primary">Primary Button</Button>
                <Button bsStyle="success">Success Button</Button>
                <Button bsStyle="info">Info Button</Button>
                <Button bsStyle="warning">Warning Button</Button>
                <Button bsStyle="danger">Danger Button</Button>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <Button
                  bsStyle="info"
                  className="btn-rounded btn-outline"
                >
                  Info Button
                </Button>
                <Button
                  bsStyle="primary"
                  className="btn-rounded btn-outline"
                >
                  Primary Button
                </Button>
                <Button
                  bsStyle="success"
                  className="btn-rounded btn-outline"
                >
                  Success Button
                </Button>
                <Button
                  bsStyle="warning"
                  className="btn-rounded btn-outline"
                >
                  Warning Button
                </Button>
                <Button
                  bsStyle="danger"
                  className="btn-rounded btn-outline"
                >
                  Danger Button
                </Button>
              </ButtonToolbar>
            </Panel>
          </div>
        </div>

        <div className="row">
          <div className="col-sm-12">
            <Panel
              header={<span>Loading Buttons</span>}
              bsStyle="primary"
            >
              <ButtonToolbar>
                <button
                  className="progress-button"
                  data-style="fill"
                  data-horizontal
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="fill"
                  data-vertical
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="shrink"
                  data-horizontal
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="shrink"
                  data-vertical
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="rotate-side-down"
                  data-perspective data-horizontal
                >Submit</button>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <button
                  className="progress-button"
                  data-style="rotate-angle-bottom"
                  data-perspective
                  data-horizontal
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="rotate-side-left"
                  data-perspective
                  data-vertical
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="rotate-side-right"
                  data-perspective
                  data-vertical
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="rotate-angle-left"
                  data-perspective
                  data-vertical
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="rotate-angle-right"
                  data-perspective
                  data-vertical
                >Submit</button>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <button
                  className="progress-button"
                  data-style="rotate-side-up" data-perspective data-horizontal
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="rotate-back" data-perspective data-horizontal
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="flip-open" data-perspective data-horizontal
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="slide-down" data-horizontal
                >Submit</button>&nbsp;
                <button
                  className="progress-button"
                  data-style="move-up" data-horizontal
                >Submit</button>
              </ButtonToolbar>
            </Panel>
          </div>
        </div>

        <div className="row">
          <div className="col-md-4">
            <Panel
              header={<span>Button Sizes</span>}
              bsStyle="primary"
            >
              <p>
                <Button bsStyle="danger" bsSize="xsmall" className="btn-rounded">
                  Extra Small Button
                </Button>
              </p>
              <p>
                <Button bsStyle="success" bsSize="small" className="btn-rounded">
                  Small Button
                </Button>
              </p>
              <p>
                <Button bsStyle="info" bsSize="large" className="btn-rounded">
                  Large Button
                </Button>
              </p>
              <p>
                <Button bsStyle="warning" className="btn-rounded" block>
                  Block Level Button
                </Button>
              </p>

            </Panel>
          </div>
          <div className="col-md-8">
            <Panel
              header={<span>Button Groups</span>}
              bsStyle="primary"
            >
              <ButtonGroup>
                <Button bsStyle="primary">Primary Button</Button>
                <Button bsStyle="default">Default Button</Button>
              </ButtonGroup>
              <hr />
              <h4>Justified Button Groups</h4>
              <div className="btn-group btn-group-justified" role="group">
                <div className="btn-group" role="group">
                  <button type="button" className="btn btn-primary">Primary </button>
                </div>
                <div className="btn-group" role="group">
                  <button type="button" className="btn btn-info">Info</button>
                </div>
                <div className="btn-group" role="group">
                  <button type="button" className="btn btn-warning">Warning</button>
                </div>
                <div className="btn-group" role="group">
                  <button type="button" className="btn btn-danger">Danger</button>
                </div>
              </div>
            </Panel>
          </div>
        </div>

        <div className="row">
          <div className="col-sm-12">
            <Panel
              header={<span>Buttons with Icons</span>}
              bsStyle="primary"
            >
              <Button bsStyle="primary" bsSize="large" className={`${s.btnCircle}`}>
                <i className="fa fa-twitter" />
              </Button>
              <Button bsStyle="info" bsSize="large" className={`${s.btnCircle}`}>
                <i className="fa fa-facebook" />
              </Button>
              <Button bsStyle="warning" className="btn-rounded">
                Download&nbsp;&nbsp;<i className="fa fa-download" />
              </Button>
              <Button bsStyle="success" className="btn-rounded btn-outline">
                <i className="fa fa-plus" />&nbsp;&nbsp;View More
              </Button>
              <Button bsStyle="danger"><i className="fa fa-phone" />&nbsp;&nbsp;Call Us!
              </Button>
            </Panel>
          </div>
        </div>

      </div>
    );
  }
}

export default withStyles(s)(Buttons);
