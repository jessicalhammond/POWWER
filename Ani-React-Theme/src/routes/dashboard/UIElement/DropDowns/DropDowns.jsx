import React, { Component, PropTypes } from 'react';
import { Panel, Nav, NavItem, NavDropdown, MenuItem, Form, FormControl, FormGroup,
          Button, DropdownButton, ButtonToolbar, SplitButton } from 'react-bootstrap';
import Navbar, { Collapse as NavbarCollapse } from 'react-bootstrap/lib/Navbar';
import Dropdown, {
  Menu as DropDownMenu,
  Toggle as DropDownToggle,
} from 'react-bootstrap/lib/Dropdown';

const title = 'DropDowns';

class DropDowns extends Component {

  static contextTypes = {
    setTitle: PropTypes.func.isRequired,
  };

  componentWillMount() {
    this.context.setTitle(title);
  }
  render() {
    return (
      <div className="animate">
        <div className="row">
          <div className="col-sm-12">
            <Panel
              header={<span>Navbar</span>}
              bsStyle="primary"
            >
              <h4>Default Style</h4>
              <Navbar fluid>
                <NavbarCollapse>
                  <Nav>
                    <NavItem className="active" eventKey={1} href="#">Link</NavItem>
                    <NavItem eventKey={2} href="#">Link</NavItem>
                    <NavDropdown eventKey={3} title="Dropdown" id="collapsible-nav-dropdown-1">
                      <MenuItem eventKey="1">Action</MenuItem>
                      <MenuItem eventKey="2">Another action</MenuItem>
                      <MenuItem eventKey="3">Something else here</MenuItem>
                      <MenuItem divider />
                      <MenuItem eventKey="4">Separated link</MenuItem>
                      <MenuItem divider />
                      <MenuItem eventKey="4">One more separated link</MenuItem>
                    </NavDropdown>
                  </Nav>
                  <Form className="navbar-form navbar-left">
                    <FormGroup>
                      <FormControl type="text" placeholder="Search" />
                      &nbsp;
                      <Button type="submit"> Submit </Button>
                    </FormGroup>
                  </Form>
                  <Nav pullRight>
                    <NavItem eventKey={1} href="#">Link</NavItem>
                    <NavDropdown eventKey={2} title="Dropdown" id="collapsible-nav-dropdown-2">
                      <MenuItem eventKey="1">Action</MenuItem>
                      <MenuItem eventKey="2">Another action</MenuItem>
                      <MenuItem eventKey="3">Something else here</MenuItem>
                      <MenuItem divider />
                      <MenuItem eventKey="4">Separated link</MenuItem>
                    </NavDropdown>
                  </Nav>
                </NavbarCollapse>
              </Navbar>

              <h4>Inverted Style</h4>
              <Navbar inverse fluid>
                <NavbarCollapse>
                  <Nav>
                    <NavItem className="active" eventKey={1} href="#">Link</NavItem>
                    <NavItem eventKey={1} href="#">Link</NavItem>
                    <NavDropdown eventKey={3} title="Dropdown" id="collapsible-nav-dropdown-3">
                      <MenuItem eventKey="1">Action</MenuItem>
                      <MenuItem eventKey="2">Another action</MenuItem>
                      <MenuItem eventKey="3">Something else here</MenuItem>
                      <MenuItem divider />
                      <MenuItem eventKey="4">Separated link</MenuItem>
                      <MenuItem divider />
                      <MenuItem eventKey="4">One more separated link</MenuItem>
                    </NavDropdown>
                  </Nav>
                  <Form className="navbar-form navbar-left">
                    <FormGroup>
                      <FormControl type="text" placeholder="Search" />
                      &nbsp;
                      <Button type="submit"> Submit </Button>
                    </FormGroup>
                  </Form>
                  <Nav pullRight>
                    <NavItem eventKey={1} href="#">Link</NavItem>
                    <NavDropdown eventKey={2} title="Dropdown" id="collapsible-nav-dropdown-4">
                      <MenuItem eventKey="1">Action</MenuItem>
                      <MenuItem eventKey="2">Another action</MenuItem>
                      <MenuItem eventKey="3">Something else here</MenuItem>
                      <MenuItem divider />
                      <MenuItem eventKey="4">Separated link</MenuItem>
                    </NavDropdown>
                  </Nav>
                </NavbarCollapse>
              </Navbar>
            </Panel>
          </div>
        </div>

        <div className="row">
          <div className="col-sm-12">
            <Panel
              header={<span>Regular Dropdown</span>}
              bsStyle="warning"
            >
              <ButtonToolbar>
                <DropdownButton id="bg-nested-dropdown" title="Button dropdown">
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton id="bg-nested-dropdown" title="Button dropdown" bsStyle="primary" >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton id="bg-nested-dropdown" title="Button dropdown" bsStyle="success" >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton id="bg-nested-dropdown" title="Button dropdown" bsStyle="warning" >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton id="bg-nested-dropdown" title="Button dropdown" bsStyle="danger" >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  className="btn-rounded"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="primary"
                  className="btn-rounded"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="success"
                  className="btn-rounded"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="warning"
                  className="btn-rounded"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="danger"
                  className="btn-rounded"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  className="btn-rounded btn-bordered"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="primary"
                  className="btn-rounded btn-bordered"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="success"
                  className="btn-rounded btn-bordered"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="warning"
                  className="btn-rounded btn-bordered"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
                <DropdownButton
                  id="bg-nested-dropdown"
                  title="Button dropdown"
                  bsStyle="danger"
                  className="btn-rounded btn-bordered"
                >
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </DropdownButton>
              </ButtonToolbar>
            </Panel>
          </div>
        </div>

        <div className="row">
          <div className="col-sm-12">
            <Panel
              header={<span>Split-Button Dropdown</span>}
              bsStyle="success"
            >
              <ButtonToolbar id="button-tool-bar-1">
                <SplitButton id="SplitButton1" bsStyle="default" title="Action">
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </SplitButton>
                <SplitButton id="SplitButton1" bsStyle="success" title="Action">
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </SplitButton>
                <SplitButton id="SplitButton1" bsStyle="primary" title="Action">
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </SplitButton>
                <SplitButton id="SplitButton1" bsStyle="warning" title="Action">
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </SplitButton>
                <SplitButton id="SplitButton1" bsStyle="danger" title="Action">
                  <MenuItem eventKey="1">Action</MenuItem>
                  <MenuItem eventKey="2">Another action</MenuItem>
                  <MenuItem eventKey="3">Something else here</MenuItem>
                  <MenuItem divider />
                  <MenuItem eventKey="4">Separated link</MenuItem>
                </SplitButton>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <Dropdown id="dropdown-1">
                  <Button bsStyle="default" className="btn-rounded">
                  Action
                  </Button>
                  <DropDownToggle id="dropdown-1" bsStyle="default" className="btn-rounded" />
                  <DropDownMenu id="dropdown-1-2">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-2">
                  <Button bsStyle="success" className="btn-rounded">
                  Action
                  </Button>
                  <DropDownToggle id="dropdown-2" bsStyle="success" className="btn-rounded" />
                  <DropDownMenu id="dropdown-2-1" >
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-3">
                  <Button bsStyle="primary" className="btn-rounded">
                  Action
                  </Button>
                  <DropDownToggle id="dropdown-3" bsStyle="primary" className="btn-rounded" />
                  <DropDownMenu id="dropdown-3">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-4">
                  <Button bsStyle="warning" className="btn-rounded">
                  Action
                  </Button>
                  <DropDownToggle id="dropdown-4" bsStyle="warning" className="btn-rounded" />
                  <DropDownMenu id="dropdown-4" >
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-5">
                  <Button bsStyle="danger" className="btn-rounded">
                  Action
                  </Button>
                  <DropDownToggle id="dropdown-5" bsStyle="danger" className="btn-rounded" />
                  <DropDownMenu id="dropdown-5">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
              </ButtonToolbar>
              <hr />
              <ButtonToolbar>
                <Dropdown id="dropdown-6">
                  <Button bsStyle="default" className="btn-rounded btn-bordered">
                  Action
                  </Button>
                  <DropDownToggle
                    id="dropdown-6"
                    bsStyle="default"
                    className="btn-rounded btn-bordered"
                  />
                  <DropDownMenu id="dropdown-6">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-7">
                  <Button bsStyle="success" className="btn-rounded btn-bordered">
                  Action
                  </Button>
                  <DropDownToggle
                    id="dropdown-7"
                    bsStyle="success"
                    className="btn-rounded btn-bordered"
                  />
                  <DropDownMenu id="dropdown-7">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-8">
                  <Button bsStyle="primary" className="btn-rounded btn-bordered">
                  Action
                  </Button>
                  <DropDownToggle
                    id="dropdown-8"
                    bsStyle="primary"
                    className="btn-rounded btn-bordered"
                  />
                  <DropDownMenu id="dropdown-8">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-9">
                  <Button bsStyle="warning" className="btn-rounded btn-bordered">
                  Action
                  </Button>
                  <DropDownToggle
                    id="dropdown-9"
                    bsStyle="warning"
                    className="btn-rounded btn-bordered"
                  />
                  <DropDownMenu id="dropdown-9">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
                <Dropdown id="dropdown-10">
                  <Button bsStyle="danger" className="btn-rounded btn-bordered">
                  Action
                  </Button>
                  <DropDownToggle
                    id="dropdown-10"
                    bsStyle="danger"
                    className="btn-rounded btn-bordered"
                  />
                  <DropDownMenu id="dropdown-10">
                    <MenuItem eventKey="1">Action</MenuItem>
                    <MenuItem eventKey="2">Another action</MenuItem>
                    <MenuItem eventKey="3" active>Active Item</MenuItem>
                    <MenuItem divider />
                    <MenuItem eventKey="4">Separated link</MenuItem>
                  </DropDownMenu>
                </Dropdown>
              </ButtonToolbar>
            </Panel>
          </div>
        </div>
      </div>
    );
  }
}
export default DropDowns;
