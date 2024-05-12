import React, { Component, PropTypes } from 'react';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import FormControl from 'react-bootstrap/lib/FormControl';
// import NProgress from 'nprogress';
import {
  Panel, Col, Form, FormGroup, ControlLabel, Button, Checkbox,
} from 'react-bootstrap';

// import s from '../../../../components/common/styles/widgets/underLine.css';

import s from './Elements.css';

const title = 'Form Elements';

class FormElements extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }

  componentDidMount() {
    // NProgress.done();
  }

  render() {
    return (
      <div className={`row animate ${s.formElements}`}>

        <div className="col-md-6">
          <Panel
            header={<span>Default Form</span>}
            bsStyle="primary"
          >
            <Form>
              <FormGroup controlId="defaultEmail" >
                <ControlLabel>Text Input</ControlLabel>
                <FormControl type="email" placeholder="Enter Email" />
              </FormGroup>
              <FormGroup controlId="defaultPassword" >
                <ControlLabel>Password</ControlLabel>
                <FormControl type="password" placeholder="Enter Password" />
              </FormGroup>
              <Checkbox> Remember Me </Checkbox>
              <Button type="submit"> Submit </Button>
            </Form>
          </Panel>

          <Panel
            header={<span>Horizontal Form</span>}
            bsStyle="success"
          >
            <Form horizontal>
              <FormGroup controlId="formHorizontalEmail">
                <Col className="text-right" sm={2}>
                  Email
                </Col>
                <Col sm={10}>
                  <FormControl type="email" placeholder="Email" />
                </Col>
              </FormGroup>

              <FormGroup controlId="formHorizontalPassword">
                <Col className="text-right" sm={2}>
                  Password
                </Col>
                <Col sm={10}>
                  <FormControl type="password" placeholder="Password" />
                </Col>
              </FormGroup>

              <FormGroup>
                <Col smOffset={2} sm={10}>
                  <Checkbox>Remember me</Checkbox>
                </Col>
              </FormGroup>

              <FormGroup>
                <Col smOffset={2} sm={10}>
                  <Button type="submit">
                    Sign in
                  </Button>
                </Col>
              </FormGroup>
            </Form>
          </Panel>

          <Panel
            header={<span>Inline Form</span>}
            bsStyle="info"
          >
            <Form inline>
              <Col sm={4}>
                <FormGroup controlId="defaultEmail" >
                  <ControlLabel>Text Input</ControlLabel>
                  <FormControl type="email" placeholder="Enter Email" />
                </FormGroup>
              </Col>
              <Col sm={4}>
                <FormGroup controlId="defaultPassword" >
                  <ControlLabel>Password</ControlLabel>
                  <FormControl type="password" placeholder="Enter Password" />
                </FormGroup>
              </Col>
              <Col sm={4}>
                <Checkbox> Remember Me </Checkbox>
                <Button type="submit"> Submit </Button>
              </Col>
            </Form>
          </Panel>
        </div>

        <div className="col-md-6">
          <Panel
            header={<span>UnderLine Form</span>}
            bsStyle="danger"
          >
            <Form>
              <FormGroup controlId="defaultEmail" >
                <ControlLabel>Text Input</ControlLabel>
                <FormControl type="email" placeholder="Enter Email" className={s.underline} />
              </FormGroup>
              <FormGroup controlId="defaultPassword" >
                <ControlLabel>Password</ControlLabel>
                <FormControl
                  type="password"
                  placeholder="Enter Password"
                  className={s.underline}
                />
              </FormGroup>
              <Checkbox> Remember Me </Checkbox>
              <Button type="submit"> Submit </Button>
            </Form>
          </Panel>

          <Panel
            header={<span>Horizontal UnderLine Form</span>}
            bsStyle="warning"
          >
            <Form horizontal>
              <FormGroup controlId="formHorizontalEmail">
                <Col className="text-right" sm={2}>
                  Email
                </Col>
                <Col sm={10}>
                  <FormControl type="email" placeholder="Email" className={s.underline} />
                </Col>
              </FormGroup>

              <FormGroup controlId="formHorizontalPassword">
                <Col className="text-right" sm={2}>
                  Password
                </Col>
                <Col sm={10}>
                  <FormControl type="password" placeholder="Password" className={s.underline} />
                </Col>
              </FormGroup>

              <FormGroup>
                <Col smOffset={2} sm={10}>
                  <Checkbox>Remember me</Checkbox>
                </Col>
              </FormGroup>

              <FormGroup>
                <Col smOffset={2} sm={10}>
                  <Button type="submit">
                    Sign in
                  </Button>
                </Col>
              </FormGroup>
            </Form>
          </Panel>

          <Panel
            header={<span>Inline UnderLine Form</span>}
            bsStyle="info"
          >
            <Form inline>
              <Col sm={4}>
                <FormGroup controlId="defaultEmail" >
                  <ControlLabel>Text Input</ControlLabel>
                  <FormControl type="email" placeholder="Enter Email" className={s.underline} />
                </FormGroup>
              </Col>
              <Col sm={4}>
                <FormGroup controlId="defaultPassword" >
                  <ControlLabel>Password</ControlLabel>
                  <FormControl
                    type="password"
                    placeholder="Enter Password"
                    className={s.underline}
                  />
                </FormGroup>
              </Col>
              <Col sm={4}>
                <Checkbox> Remember Me </Checkbox>
                <Button type="submit"> Submit </Button>
              </Col>
            </Form>
          </Panel>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(FormElements);
