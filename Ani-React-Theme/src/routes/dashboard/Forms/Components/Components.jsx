import React, { Component, PropTypes } from 'react';
import DatePicker from 'react-datepicker';
import moment from 'moment';

import { Panel, Form, FormGroup, Col, HelpBlock, ControlLabel,
  Checkbox, Radio, Button } from 'react-bootstrap';
import FormControl, { Static as FormControlStatic } from 'react-bootstrap/lib/FormControl';
import InputGroup,
{ Addon as InputGroupAddon,
  Button as InputGroupButton,
} from 'react-bootstrap/lib/InputGroup';
import withStyles from 'isomorphic-style-loader/lib/withStyles';
import s from './Components.css';

const title = 'Form Component';

class FormComponent extends Component {
  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      switchState: true,
      startDate: moment(),
    };
    this.handleChange = this.handleChange.bind(this);
  }
  componentWillMount() {
    this.context.setTitle(title);
  }
  handleChange(date) {
    this.setState({
      startDate: date,
    });
  }
  render() {
    return (
      <div className="row animate" id="formComponent">
        <div className="col-md-12">
          <Panel
            header={<span>Components</span>}
            bsStyle="primary"
          >
            <Form horizontal>
              <FormGroup controlId="normal">
                <Col className="text-right" sm={2}> Normal </Col>
                <Col sm={10}>
                  <FormControl type="text" placeholder="" />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="password">
                <Col className="text-right" sm={2}> Password </Col>
                <Col sm={10}>
                  <FormControl type="password" placeholder="password" />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="helpText" >
                <Col className="text-right" sm={2}> Help Text </Col>
                <Col sm={10}>
                  <FormControl type="text" placeholder="Help Text" />
                  <HelpBlock>Example block-level help text here.</HelpBlock>
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="placeholder">
                <Col className="text-right" sm={2}> Place Holder </Col>
                <Col sm={10}>
                  <FormControl type="input" placeholder="placeholder" />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="lineForm">
                <Col className="text-right" sm={2}> Line </Col>
                <Col sm={10}>
                  <FormControl type="input" placeholder="line" className={s.underline} />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="disabledForm">
                <Col className="text-right" sm={2}> Disabled </Col>
                <Col sm={10}>
                  <FormControl type="input" placeholder="Disabled Input Form" disabled />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="staticForm">
                <Col className="text-right" sm={2}> Static Text </Col>
                <Col sm={10}>
                  <FormControlStatic>
                   email@example.com
                  </FormControlStatic>
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="checkboxAndRadio">
                <Col className="text-right" sm={2} style={{ marginTop: '13px' }}>
                  Checkbox and Radio
                </Col>
                <Col sm={10}>
                  <Checkbox>
                    Option one is this and that—be sure to include why it's great
                  </Checkbox>
                  <Checkbox disabled>
                    Option two is disabled
                  </Checkbox>
                  <Radio>
                    Option one is this and that—be sure to include why it's great
                  </Radio>
                  <Radio>
                    Option two can be something else and selecting it will deselect option one
                  </Radio>
                  <Radio disabled>
                    Option three is disabled
                  </Radio>
                </Col>
              </FormGroup>
              <hr />
              <div className="row">
                <Col sm={2} className="text-right"> Button Switch </Col>
                <Col sm={10}>
                  <label className="switch" htmlFor="toggleCheckboxSquare">
                    <input id="toggleCheckboxSquare" type="checkbox" />
                    <div className="slider" />
                  </label> &nbsp;
                  <label className="switch" htmlFor="toggleCheckboxRound">
                    <input id="toggleCheckboxRound" type="checkbox" />
                    <div className="slider round" />
                  </label>
                </Col>
              </div>
              <hr />
              <div className="row">
                <Col htmlFor="inputtext" sm={2} className="m-t-0">
                  Custom Checkboxes and Radio Buttons
                </Col>
                <Col sm={2}>
                  <label className="checkbox1" htmlFor="Option">
                    <input id="Option" type="checkbox" className="" />
                    <span />
                  </label>
                  <label className="radio1" htmlFor="Option1">
                    <input id="Option1" name="one" type="radio" className="" />
                    <span />
                  </label>
                  <label className="radio1" htmlFor="Option2">
                    <input id="Option2" name="one" type="radio" className="" />
                    <span />
                  </label>
                  <label className="radio1" htmlFor="Option3">
                    <input id="Option3" name="one" type="radio" className="" />
                    <span />
                  </label>
                </Col>
              </div>
              <hr />
              <FormGroup controlId="select">
                <Col className="text-right" sm={2}> Select </Col>
                <Col sm={10}>
                  <FormControl componentClass="select" placeholder="select">
                    <option value="1">1</option>
                    <option value="1">2</option>
                    <option value="1">3</option>
                    <option value="1">4</option>
                    <option value="1">5</option>
                  </FormControl>
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="multipleSelect">
                <Col className="text-right" sm={2}> Multiple Select </Col>
                <Col sm={10}>
                  <FormControl componentClass="select" placeholder=" Multiple Select" multiple>
                    <option value="1">1</option>
                    <option value="1">2</option>
                    <option value="1">3</option>
                    <option value="1">4</option>
                    <option value="1">5</option>
                  </FormControl>
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="inputSuccess" validationState="success">
                <Col className="text-right" sm={2}>
                  <ControlLabel>Input with success</ControlLabel>
                </Col>
                <Col sm={10}>
                  <FormControl type="text" placeholder="" />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="inputWarning" validationState="warning">
                <Col className="text-right" sm={2}>
                  <ControlLabel>Input with warning</ControlLabel>
                </Col>
                <Col sm={10}>
                  <FormControl type="text" placeholder="" />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="inputDanger" validationState="error">
                <Col className="text-right" sm={2}>
                  <ControlLabel>Input with danger</ControlLabel>
                </Col>
                <Col sm={10}>
                  <FormControl type="text" placeholder="" />
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="controlSize">
                <Col className="text-right" sm={2}>
                  Control Sizing
                </Col>
                <Col sm={10} id="columnSize">
                  <FormGroup controlId="controlSizeLarge" bsSize="lg">
                    <FormControl type="text" placeholder="Large Input Form" />
                  </FormGroup>
                  <FormGroup controlId="controlSizeNormal">
                    <FormControl type="text" placeholder="Normal Input Form" />
                  </FormGroup>
                  <FormGroup controlId="controlSizeLarge" bsSize="sm">
                    <FormControl type="text" placeholder="Small Input Form" />
                  </FormGroup>
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="columnSize">
                <Col className="text-right" sm={2}>
                  Column Sizing
                </Col>
                <Col sm={2}>
                  <FormControl type="text" placeholder="col-sm-2" />
                </Col>
                <Col sm={3}>
                  <FormControl type="text" placeholder="col-sm-3" />
                </Col>
                <Col sm={5}>
                  <FormControl type="text" placeholder="col-sm-5" />
                </Col>
              </FormGroup>
              <hr />

              <FormGroup controlId="inputGroup">
                <Col className="text-right" sm={2}> Input Group </Col>
                <Col sm={10}>
                  <InputGroup>
                    <InputGroupAddon>@</InputGroupAddon>
                    <FormControl type="text" placeholder="UserName" />
                  </InputGroup>
                  <br />
                  <InputGroup>
                    <FormControl type="text" placeholder="" />
                    <InputGroupAddon>.00</InputGroupAddon>
                  </InputGroup>
                  <br />
                  <InputGroup>
                    <InputGroupAddon>$</InputGroupAddon>
                    <FormControl type="text" placeholder="" />
                    <InputGroupAddon>.00</InputGroupAddon>
                  </InputGroup>
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="buttonAddons">
                <Col className="text-right" sm={2}> Button Addons </Col>
                <Col sm={10}>
                  <InputGroup>
                    <InputGroupButton><Button> Go! </Button> </InputGroupButton>
                    <FormControl type="text" placeholder="" />
                  </InputGroup>
                  <br />
                  <InputGroup>
                    <FormControl type="text" placeholder="" />
                    <InputGroupButton><Button> Go </Button> </InputGroupButton>
                  </InputGroup>
                </Col>
              </FormGroup>
              <hr />
              <FormGroup controlId="datePicker">
                <Col className="text-right mt-5" sm={2} style={{ marginTop: '13px' }}>
                   Date Picker
                </Col>
                <Col sm={10}>
                  <DatePicker
                    className="form-control"
                    selected={this.state.startDate}
                    onChange={this.handleChange}
                  />
                </Col>
              </FormGroup>
            </Form>
          </Panel>
        </div>
      </div>
    );
  }
}

export default withStyles(s)(FormComponent);
